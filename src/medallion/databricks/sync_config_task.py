"""Job task `sync_config` (`medallion-sync-config --config-dir DIR`): first task of every Run.

Only wires adapters and calls the use case; DIR is the config delivered by the bundle. It also
creates the runtime tables and publishes the active Ingestions as the task value
`active_ingestions`, the input of the for-each task.
"""

import argparse

from pyspark.sql import SparkSession

from medallion.controller.clock import SystemClock
from medallion.controller.config import load_config
from medallion.controller.sync import sync_config
from medallion.databricks.controller_tables import DeltaControllerTables
from medallion.databricks.runtime_tables import DeltaIngestionCatalog, ensure_runtime_tables


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="medallion-sync-config")
    parser.add_argument("--config-dir", required=True)
    args = parser.parse_args(argv)

    clock = SystemClock()
    config = load_config(args.config_dir, clock)
    spark = SparkSession.builder.getOrCreate()
    tables = DeltaControllerTables(spark, config.catalog_structure.controller)
    result = sync_config(config, tables, clock)
    ensure_runtime_tables(spark, config)
    active = [i.name for i in DeltaIngestionCatalog(spark, config).active()]
    print(f"sync_config: {result.ingestions} ingestions, {result.connections} connections "
          f"→ {tables.schema}; active: {active}")

    from databricks.sdk.runtime import dbutils  # only resolvable inside a job run

    dbutils.jobs.taskValues.set(key="active_ingestions", value=active)
