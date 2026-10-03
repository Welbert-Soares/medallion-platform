"""sql/table Ingestions through `run_ingestion`: Connection, secret, incremental query."""

from datetime import datetime, timezone

import pytest
from fakes.clock import FixedClock
from fakes.ingestion import FakeIngestionCatalog, FakeRawWriter, FakeRunLog
from fakes.jdbc import FakeJdbcClient, FakeSecrets

from medallion.controller.config import Connection, Ingestion, PlatformConfig
from medallion.controller.connections import ConfigConnectionCatalog, secret_scopes
from medallion.controller.runtime import IngestionState
from medallion.ingestion.jdbc import JdbcReader, build_query, jdbc_url
from medallion.ingestion.run import run_ingestion

NOW = datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc)
PASSWORD = "s3cr3t-p4ss"
PG = {"id": "pg", "subprotocol": "postgresql", "driver": "org.postgresql.Driver",
      "host": "db.example", "port": 5432, "database": "shop", "user": "reader",
      "secret_scope": "medallion-platform", "secret_key": "pg_password"}
CONFIG = PlatformConfig.model_validate({
    "catalog_structure": {
        "layers": [{"name": "raw", "catalog": "medallion_raw"}],
        "controller": {"catalog": "medallion_controller", "schema": {"name": "controller"}},
        "source_systems": [{"name": "orders"}]},
    "connections": [PG],
})
SQL = Ingestion.model_validate({
    "name": "orders_postgres", "source_system": "orders", "raw_table": "orders", "active": True,
    "source": {"type": "sql", "connection_id": "pg", "query": "SELECT * FROM orders",
               "filter": "status <> 'test'", "watermark_column": "updated_at",
               "partition_column": "order_id"}})
TABLE = Ingestion.model_validate({
    "name": "orders_customers", "source_system": "orders", "raw_table": "customers",
    "active": True, "source": {"type": "table", "connection_id": "pg", "schema": "public",
                               "table": "customers", "watermark_column": "updated_at"}})
ROWS = [{"order_id": 1, "updated_at": "2026-10-02T10:00:00"},
        {"order_id": 2, "updated_at": "2026-10-02T11:30:00"}]


class Harness:
    def __init__(self, ingestion, watermark=None, rows=ROWS, password=PASSWORD):
        states = [IngestionState(ingestion.name, watermark, 1, NOW)] if watermark else []
        self.catalog = FakeIngestionCatalog([ingestion], states)
        self.secrets = FakeSecrets({("medallion-platform", "pg_password"): password})
        self.client = FakeJdbcClient(rows)
        self.writer = FakeRawWriter()
        self.log = FakeRunLog()
        self.ingestion = ingestion

    def run(self):
        reader = JdbcReader(ConfigConnectionCatalog(CONFIG), self.secrets, self.client)
        return run_ingestion(self.ingestion.name, 7, 8, catalog=self.catalog,
                             readers={"sql": reader, "table": reader}, writer=self.writer,
                             run_log=self.log, clock=FixedClock(NOW))

    @property
    def request(self):
        (request,) = self.client.requests
        return request


# ── through run_ingestion ────────────────────────────────────────────────────


def test_first_sql_run_reads_everything_and_sets_the_watermark():
    h = Harness(SQL)

    run = h.run()

    assert h.request.query == "SELECT * FROM (SELECT * FROM orders) src WHERE (status <> 'test')"
    assert (h.request.url, h.request.driver, h.request.user) == (
        "jdbc:postgresql://db.example:5432/shop", "org.postgresql.Driver", "reader")
    assert (h.request.partition_column, h.request.watermark_column) == ("order_id", "updated_at")
    assert (run.status, run.rows_written, run.watermark_after) == (
        "success", 2, "2026-10-02T11:30:00")
    assert h.catalog.states["orders_postgres"].watermark == "2026-10-02T11:30:00"


def test_incremental_sql_run_reads_beyond_the_watermark():
    h = Harness(SQL, watermark="2026-10-01T00:00:00")

    h.run()

    assert h.request.query == (
        "SELECT * FROM (SELECT * FROM orders) src "
        "WHERE (status <> 'test') AND updated_at > '2026-10-01T00:00:00'")


def test_incremental_table_run_selects_the_declared_table():
    h = Harness(TABLE, watermark="2026-10-01T00:00:00")

    h.run()

    assert h.request.query == (
        "SELECT * FROM (SELECT * FROM public.customers) src "
        "WHERE updated_at > '2026-10-01T00:00:00'")


def test_no_new_rows_keep_the_watermark():
    h = Harness(SQL, watermark="2026-10-02T11:30:00", rows=[])

    run = h.run()

    assert h.writer.tables == {}
    assert (run.status, run.watermark_after) == ("success", "2026-10-02T11:30:00")


def test_password_comes_from_the_secret_scope_of_the_connection():
    h = Harness(SQL)

    h.run()

    assert h.secrets.requested == [("medallion-platform", "pg_password")]
    assert h.request.password == PASSWORD


def test_password_never_reaches_logs_or_reprs():
    h = Harness(SQL)

    run = h.run()

    assert PASSWORD not in repr(h.request)
    assert PASSWORD not in repr(run)
    assert PASSWORD not in repr(h.catalog.states)


def test_missing_secret_fails_without_reading():
    h = Harness(SQL)
    h.secrets.secrets.clear()

    with pytest.raises(KeyError):
        h.run()

    assert h.client.requests == []
    assert h.log.runs[0].status == "failed"


def test_undeclared_connection_fails():
    orphan = Ingestion.model_validate({
        **SQL.model_dump(by_alias=True),
        "source": {**SQL.source.model_dump(by_alias=True), "connection_id": "nope"}})
    h = Harness(orphan)

    with pytest.raises(KeyError, match="connection 'nope' is not declared"):
        h.run()

    assert h.log.runs[0].status == "failed"


# ── query and URL building ───────────────────────────────────────────────────


def test_query_without_filter_or_watermark_is_left_untouched():
    plain = Ingestion.model_validate({**SQL.model_dump(by_alias=True), "source": {
        "type": "sql", "connection_id": "pg", "query": "SELECT 1"}})

    assert build_query(plain.source, "2026-01-01") == "SELECT 1"


def test_watermark_literal_is_escaped():
    assert build_query(TABLE.source, "x' OR '1'='1").endswith("updated_at > 'x'' OR ''1''=''1'")


@pytest.mark.parametrize(("subprotocol", "url"), [
    ("postgresql", "jdbc:postgresql://db.example:5432/shop"),
    ("mysql", "jdbc:mysql://db.example:5432/shop"),
    ("sqlserver", "jdbc:sqlserver://db.example:5432;databaseName=shop"),
    ("oracle", "jdbc:oracle:thin:@//db.example:5432/shop"),
])
def test_jdbc_url_per_subprotocol(subprotocol, url):
    assert jdbc_url(Connection.model_validate({**PG, "subprotocol": subprotocol})) == url


def test_secret_scopes_are_those_of_the_declared_connections():
    other = {**PG, "id": "ms", "secret_scope": "another"}
    config = PlatformConfig.model_validate({**CONFIG.model_dump(by_alias=True),
                                            "connections": [PG, other, {**PG, "id": "pg2"}]})

    assert secret_scopes(config) == ["another", "medallion-platform"]
