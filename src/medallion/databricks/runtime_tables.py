"""Controller runtime adapters: `IngestionCatalog` and `RunLog` over Delta tables.

Ingestion definitions come from the config deployed with the bundle (the same config that
`sync_config` mirrors); Ingestion State and the execution log live in the Controller schema.
"""

import time
from dataclasses import asdict

from pyspark.sql import SparkSession

from medallion.controller.config.model import Ingestion, PlatformConfig
from medallion.controller.runtime import (
    EXECUTION_LOG_COLUMNS,
    INGESTION_STATE_COLUMNS,
    IngestionRun,
    IngestionState,
)
from medallion.databricks.spark_tables import create_table_if_not_exists, rows_frame

_MERGE_ATTEMPTS = 5


def ensure_runtime_tables(spark: SparkSession, config: PlatformConfig) -> None:
    """Create `ingestion_state` and `execution_log` once, before Ingestion Runs start in parallel."""
    controller = config.catalog_structure.controller
    create_table_if_not_exists(
        spark, controller.table("ingestion_state"), INGESTION_STATE_COLUMNS,
        "Watermark and last Run of each Ingestion; written only by the runtime (ADR 0003)",
    )
    create_table_if_not_exists(
        spark, controller.table("execution_log"), EXECUTION_LOG_COLUMNS,
        "One row per Ingestion Run",
    )


class DeltaIngestionCatalog:
    def __init__(self, spark: SparkSession, config: PlatformConfig):
        self.spark = spark
        self.config = config
        self.state_table = config.catalog_structure.controller.table("ingestion_state")

    def active(self) -> list[Ingestion]:
        return [i for i in self.config.ingestions if i.active]

    def get(self, name: str) -> Ingestion:
        try:
            return self.config.ingestion(name)
        except StopIteration:
            raise KeyError(f"ingestion '{name}' is not declared in config") from None

    def raw_table_of(self, ingestion: Ingestion) -> str:
        return self.config.raw_table_of(ingestion)

    def get_state(self, name: str) -> IngestionState | None:
        rows = (self.spark.table(self.state_table)
                .where(f"ingestion_name = '{name}'").limit(1).collect())
        return IngestionState(**rows[0].asDict()) if rows else None

    def save_state(self, state: IngestionState) -> None:
        source = rows_frame(self.spark, INGESTION_STATE_COLUMNS, [asdict(state)])
        source.createOrReplaceTempView("_ingestion_state_update")
        statement = (
            f"MERGE INTO {self.state_table} t USING _ingestion_state_update s "
            "ON t.ingestion_name = s.ingestion_name "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
        for attempt in range(1, _MERGE_ATTEMPTS + 1):
            try:
                self.spark.sql(statement)
                return
            except Exception as e:  # concurrent Ingestion Runs may conflict on the first insert
                if "Concurrent" not in type(e).__name__ or attempt == _MERGE_ATTEMPTS:
                    raise
                time.sleep(attempt)


class DeltaRunLog:
    def __init__(self, spark: SparkSession, config: PlatformConfig):
        self.spark = spark
        self.table = config.catalog_structure.controller.table("execution_log")

    def record(self, run: IngestionRun) -> None:
        df = rows_frame(self.spark, EXECUTION_LOG_COLUMNS, [asdict(run)])
        df.write.mode("append").saveAsTable(self.table)
