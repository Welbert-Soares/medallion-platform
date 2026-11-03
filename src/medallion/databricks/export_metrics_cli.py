"""`medallion-export-metrics`: write `metrics.json` from `run_metrics` (SQL warehouse) + config.

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
from medallion.controller.metrics import platform_metrics, run_metrics_table

_INTEGER_TYPES = {"BYTE", "SHORT", "INT", "LONG"}


def _typed(value: str | None, type_name: str):
    if value is None:
        return None
    if type_name in _INTEGER_TYPES:
        return int(value)
    if type_name == "DECIMAL":
        return float(Decimal(value))
    return value


def fetch_rows(client: WorkspaceClient, warehouse_id: str, table: str) -> list[dict]:
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=f"SELECT * FROM {table}", wait_timeout="50s")
    error = response.status.error
    if error and "TABLE_OR_VIEW_NOT_FOUND" in (error.message or ""):
        # The first Run after a fresh deploy creates the table; until then there is no history.
        print(f"{table} does not exist yet: publishing metrics without Runs", file=sys.stderr)
        return []
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"query on {table} ended {response.status.state}: {response.status}")
    columns = [(c.name, c.type_name.value) for c in response.manifest.schema.columns]
    return [{name: _typed(value, type_name) for (name, type_name), value in zip(columns, row)}
            for row in (response.result.data_array or [])]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="medallion-export-metrics", description=__doc__)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--config-dir", default="config/platform")
    parser.add_argument("--manifest", type=Path, help="dbt target/manifest.json, for dbt counts")
    args = parser.parse_args(argv)

    config = load_config(args.config_dir)
    rows = fetch_rows(WorkspaceClient(), args.warehouse_id, run_metrics_table(config))
    manifest = (json.loads(args.manifest.read_text())
                if args.manifest and args.manifest.is_file() else None)
    metrics = platform_metrics(config, rows, datetime.now(UTC), manifest)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(metrics, ensure_ascii=False, indent=2) + "\n")

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
