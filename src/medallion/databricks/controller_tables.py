"""`ControllerTables` adapter: overwrites the config mirror tables as Delta tables."""

from pyspark.sql import SparkSession

from medallion.controller.config.model import Controller
from medallion.controller.sync import CONNECTION_COLUMNS, INGESTION_COLUMNS, Row

_COMMENTS = {
    "ingestions": "Mirror of the Ingestions declared in config/platform; replaced on every sync",
    "connections": "Mirror of config/platform/connections.yml; address and secret reference only",
}


class DeltaControllerTables:
    def __init__(self, spark: SparkSession, controller: Controller):
        self.spark = spark
        self.schema = f"{controller.catalog}.{controller.schema_.name}"

    def replace_ingestions(self, rows: list[Row]) -> None:
        self._overwrite("ingestions", INGESTION_COLUMNS, rows)

    def replace_connections(self, rows: list[Row]) -> None:
        self._overwrite("connections", CONNECTION_COLUMNS, rows)

    def _overwrite(self, table: str, columns: dict[str, str], rows: list[Row]) -> None:
        ddl = ", ".join(f"`{name}` {type_}" for name, type_ in columns.items())
        df = self.spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], ddl)
        full_name = f"{self.schema}.{table}"
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
        self.spark.sql(f"COMMENT ON TABLE {full_name} IS '{_COMMENTS[table]}'")
