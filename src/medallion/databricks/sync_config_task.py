"""Job task `sync_config` (`medallion-sync-config --config-dir DIR`): first task of every Run.

Only wires adapters and calls the use case; DIR is the config delivered by the bundle.
"""

import argparse

from pyspark.sql import SparkSession

from medallion.controller.clock import SystemClock
from medallion.controller.config import load_config
from medallion.controller.sync import sync_config
from medallion.databricks.controller_tables import DeltaControllerTables


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="medallion-sync-config")
    parser.add_argument("--config-dir", required=True)
    args = parser.parse_args(argv)

    clock = SystemClock()
    config = load_config(args.config_dir, clock)
    tables = DeltaControllerTables(
        SparkSession.builder.getOrCreate(), config.catalog_structure.controller
    )
    result = sync_config(config, tables, clock)
    print(f"sync_config: {result.ingestions} ingestions, {result.connections} connections "
          f"→ {tables.schema}")
