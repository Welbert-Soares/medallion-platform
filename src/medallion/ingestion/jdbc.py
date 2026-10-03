"""sql/table Ingestions: Connection + secret → JDBC request. Spark only behind `JdbcClient`."""

from dataclasses import dataclass, field
from typing import Protocol

from medallion.controller.config.model import Connection, Ingestion, SqlSource, TableSource
from medallion.controller.connections import ConnectionCatalog, SecretProvider
from medallion.ingestion.run import ReadResult
from medallion.ingestion.window import DateWindow


@dataclass(frozen=True)
class JdbcRequest:
    url: str
    driver: str
    user: str
    password: str = field(repr=False)  # never printed, logged or stored
    query: str
    partition_column: str | None = None
    watermark_column: str | None = None


class JdbcClient(Protocol):
    def read(self, request: JdbcRequest) -> ReadResult:
        """Run the query; ReadResult.watermark is the max of `watermark_column` read, if any."""
        ...


class JdbcReader:
    """`SourceReader` for sql and table Ingestions."""

    def __init__(self, connections: ConnectionCatalog, secrets: SecretProvider,
                 client: JdbcClient):
        self.connections = connections
        self.secrets = secrets
        self.client = client

    def fingerprint(self, ingestion: Ingestion) -> str | None:
        return None

    def read(self, ingestion: Ingestion, watermark: str | None, window: DateWindow | None
             ) -> ReadResult:
        source = ingestion.source
        if not isinstance(source, (SqlSource, TableSource)):
            raise TypeError(f"{ingestion.name} is not a sql/table ingestion")
        connection = self.connections.get(source.connection_id)
        password = self.secrets.get(connection.secret_scope, connection.secret_key)
        return self.client.read(JdbcRequest(
            url=jdbc_url(connection), driver=connection.driver, user=connection.user,
            password=password, query=build_query(source, watermark),
            partition_column=source.partition_column, watermark_column=source.watermark_column,
        ))


def jdbc_url(connection: Connection) -> str:
    host, port, db = connection.host, connection.port, connection.database
    if connection.subprotocol == "sqlserver":
        return f"jdbc:sqlserver://{host}:{port};databaseName={db}"
    if connection.subprotocol == "oracle":
        return f"jdbc:oracle:thin:@//{host}:{port}/{db}"
    return f"jdbc:{connection.subprotocol}://{host}:{port}/{db}"


def build_query(source: SqlSource | TableSource, watermark: str | None) -> str:
    """The source query, narrowed by the declared filter and by the Watermark."""
    if isinstance(source, TableSource):
        base = f"SELECT * FROM {source.schema_}.{source.table}"
    else:
        base = source.query
    conditions = []
    if source.filter:
        conditions.append(f"({source.filter})")
    if source.watermark_column and watermark is not None:
        conditions.append(f"{source.watermark_column} > {_literal(watermark)}")
    if not conditions:
        return base
    return f"SELECT * FROM ({base}) src WHERE {' AND '.join(conditions)}"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
