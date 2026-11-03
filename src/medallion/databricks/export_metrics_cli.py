"""`medallion-export-metrics`: write `metrics.json` from `run_metrics` (SQL warehouse) + config.

`metrics.json` also carries the row count of every data table per Layer (taken from the dbt
manifest), and `--series-table` publishes one gold table as-is (e.g. for the portfolio's chart).
Run by the site workflow after the daily Run. Prints `stale=true|false`, and appends it to
`$GITHUB_OUTPUT` when set, so the workflow can fail after publishing when no Run happened.
"""

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from medallion.controller.config.loader import load_config
from medallion.controller.metrics import (
    data_tables,
    platform_metrics,
    row_count_sql,
    run_metrics_table,
    series_payload,
)

_INTEGER_TYPES = {"BYTE", "SHORT", "INT", "LONG"}


def _typed(value: str | None, type_name: str):
    if value is None:
        return None
    if type_name in _INTEGER_TYPES:
        return int(value)
    if type_name == "DECIMAL":
        return float(Decimal(value))
    return value


class TableNotFound(Exception):
    pass


def query(client: WorkspaceClient, warehouse_id: str, statement: str) -> list[dict]:
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
    error = response.status.error
    if error and "TABLE_OR_VIEW_NOT_FOUND" in (error.message or ""):
        raise TableNotFound(error.message)
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"query ended {response.status.state}: {response.status}")
    columns = [(c.name, c.type_name.value) for c in response.manifest.schema.columns]
    return [{name: _typed(value, type_name) for (name, type_name), value in zip(columns, row)}
            for row in (response.result.data_array or [])]


def fetch_rows(client: WorkspaceClient, warehouse_id: str, table: str) -> list[dict]:
    try:
        return query(client, warehouse_id, f"SELECT * FROM {table}")
    except TableNotFound:
        # The first Run after a fresh deploy creates the table; until then there is no data.
        print(f"{table} does not exist yet: publishing without it", file=sys.stderr)
        return []


def count_rows(client: WorkspaceClient, warehouse_id: str, tables: list[dict]) -> list[dict]:
    if not tables:
        return []
    try:
        counts = {r["relation"]: r["row_count"]
                  for r in query(client, warehouse_id, row_count_sql(tables))}
    except TableNotFound as e:
        print(f"row counts skipped, a table does not exist yet: {e}", file=sys.stderr)
        return []
    return [{**t, "rows": counts[t["relation"]]} for t in tables]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="medallion-export-metrics", description=__doc__)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--config-dir", default="config/platform")
    parser.add_argument("--manifest", type=Path,
                        help="dbt target/manifest.json: dbt counts and the tables to count rows of")
    parser.add_argument("--series-table", help="a gold table to publish as-is, ordered by its "
                                               "first column")
    parser.add_argument("--series-out", type=Path, help="where to write --series-table")
    args = parser.parse_args(argv)

    config = load_config(args.config_dir)
    client, now = WorkspaceClient(), datetime.now(UTC)
    rows = fetch_rows(client, args.warehouse_id, run_metrics_table(config))
    manifest = (json.loads(args.manifest.read_text())
                if args.manifest and args.manifest.is_file() else None)
    table_rows = count_rows(client, args.warehouse_id, data_tables(config, manifest))
    metrics = platform_metrics(config, rows, now, manifest, table_rows)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(metrics, ensure_ascii=False, indent=2) + "\n")

    if args.series_table and args.series_out:
        series = fetch_rows(client, args.warehouse_id, f"{args.series_table} ORDER BY 1")
        args.series_out.write_text(json.dumps(series_payload(args.series_table, series, now),
                                              ensure_ascii=False) + "\n")
        print(f"{args.series_out}: {len(series)} rows of {args.series_table}")

    runs = metrics["runs"]
    stale = "true" if runs["stale"] else "false"
    print(f"{args.out}: {runs['total']} Runs, last {runs['last_run_at']} "
          f"({runs['hours_since_last_run']} h ago), stale={stale}")
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as f:
            f.write(f"stale={stale}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
