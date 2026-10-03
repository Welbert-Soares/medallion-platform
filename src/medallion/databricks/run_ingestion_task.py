"""Job task `medallion-run-ingestion`: one iteration of the for-each over active Ingestions.

Only wires adapters and calls the use case. Raises (failing the task) when the Ingestion fails.
"""

import argparse

from pyspark.sql import SparkSession

from medallion.controller.clock import SystemClock
from medallion.controller.config import load_config
from medallion.databricks.raw_writer import DeltaRawWriter
from medallion.databricks.runtime_tables import DeltaIngestionCatalog, DeltaRunLog
from medallion.databricks.sources import ApiReader, CsvReader
from medallion.ingestion.run import run_ingestion


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="medallion-run-ingestion")
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("--ingestion", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--task-run-id", type=int, required=True)
    args = parser.parse_args(argv)

    clock = SystemClock()
    config = load_config(args.config_dir, clock)
    spark = SparkSession.builder.getOrCreate()
    run = run_ingestion(
        args.ingestion, args.run_id, args.task_run_id,
        catalog=DeltaIngestionCatalog(spark, config),
        readers={"api": ApiReader(spark), "csv": CsvReader(spark)},
        writer=DeltaRawWriter(),
        run_log=DeltaRunLog(spark, config),
        clock=clock,
    )
    print(f"{run.ingestion_name}: {run.status} — read {run.rows_read}, wrote {run.rows_written}, "
          f"watermark {run.watermark_before} → {run.watermark_after}")
