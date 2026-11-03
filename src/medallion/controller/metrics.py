"""Platform metrics published as `metrics.json`: what the platform is and how its Runs went.

Built from `run_metrics` rows (one per Run, `medallion_gold.platform.run_metrics`), the config and,
when available, the dbt manifest. Also the external heartbeat: the job's own checks run inside
the job, so only a check outside it can notice that the job did not run at all.
"""

from collections.abc import Mapping, Sequence
from datetime import datetime

from medallion.controller.config.model import PlatformConfig

METRICS_SCHEMA = "platform"
METRICS_TABLE = "run_metrics"
# The job runs daily; a little slack over 24 h for queueing and a slow Run.
STALE_AFTER_HOURS = 26
RECENT_RUNS = 30


def run_metrics_table(config: PlatformConfig) -> str:
    gold = config.catalog_structure.layer("gold").catalog
    return f"{gold}.{METRICS_SCHEMA}.{METRICS_TABLE}"


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def dbt_counts(manifest: Mapping | None) -> dict[str, int] | None:
    """Models, data tests and sources of the dbt project itself (packages excluded)."""
    if not manifest:
        return None
    project = manifest.get("metadata", {}).get("project_name")
    nodes = [n for n in manifest.get("nodes", {}).values() if n.get("package_name") == project]
    return {
        "models": sum(n["resource_type"] == "model" for n in nodes),
        "tests": sum(n["resource_type"] == "test" for n in nodes),
        "sources": sum(s.get("package_name") == project
                       for s in manifest.get("sources", {}).values()),
    }


_PERSISTED = {"table", "incremental"}


def data_tables(config: PlatformConfig, manifest: Mapping | None) -> list[dict]:
    """Every table that holds data, per Layer: Raw sources and persisted models of the project.

    Views (staging) are left out: they hold no rows of their own. The Controller is not a Layer.
    """
    if not manifest:
        return []
    layer_of = {layer.catalog: layer.name for layer in config.catalog_structure.layers}
    project = manifest.get("metadata", {}).get("project_name")
    nodes = [*manifest.get("sources", {}).values(),
             *(n for n in manifest.get("nodes", {}).values()
               if n.get("resource_type") == "model"
               and n.get("config", {}).get("materialized") in _PERSISTED)]
    tables = []
    for node in nodes:
        if node.get("package_name") != project or node.get("database") not in layer_of:
            continue
        table = node.get("alias") or node.get("identifier") or node["name"]
        tables.append({"layer": layer_of[node["database"]], "schema": node["schema"],
                       "table": table,
                       "relation": f"{node['database']}.{node['schema']}.{table}"})
    order = [layer.name for layer in config.catalog_structure.layers]
    return sorted(tables, key=lambda t: (order.index(t["layer"]), t["schema"], t["table"]))


def row_count_sql(tables: Sequence[Mapping]) -> str:
    """One query counting the rows of every table (`relation`, `row_count`)."""
    return "\nUNION ALL\n".join(
        f"SELECT '{t['relation']}' AS relation, count(*) AS row_count FROM {t['relation']}"
        for t in tables)


def series_payload(table: str, rows: Sequence[Mapping], now: datetime) -> dict:
    """A gold table published as-is, for charts (e.g. the portfolio's temperature series)."""
    return {"generated_at": now.isoformat(timespec="seconds"), "table": table,
            "rows": list(rows)}


def platform_metrics(config: PlatformConfig, runs: Sequence[Mapping], now: datetime,
                     manifest: Mapping | None = None,
                     table_rows: Sequence[Mapping] = ()) -> dict:
    ordered = sorted(runs, key=lambda r: r["started_at"])
    last = ordered[-1] if ordered else None
    hours_since_last = (round((now - _timestamp(last["finished_at"])).total_seconds() / 3600, 1)
                        if last else None)
    succeeded = sum(r["status"] == "success" for r in ordered)

    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "platform": {
            "source_systems": len(config.catalog_structure.source_systems),
            "ingestions_active": sum(i.active for i in config.ingestions),
            "layers": [layer.name for layer in config.catalog_structure.layers],
            "dbt": dbt_counts(manifest),
        },
        "runs": {
            "total": len(ordered),
            "succeeded": succeeded,
            "success_rate": round(succeeded / len(ordered), 4) if ordered else None,
            "rows_written": sum(r["rows_written"] for r in ordered),
            "first_run_at": ordered[0]["started_at"] if ordered else None,
            "last_run_at": last["started_at"] if last else None,
            "hours_since_last_run": hours_since_last,
            "stale": hours_since_last is None or hours_since_last > STALE_AFTER_HOURS,
        },
        "tables": [{key: t[key] for key in ("layer", "schema", "table", "rows")}
                   for t in table_rows],
        "recent_runs": [
            {key: run[key] for key in ("run_id", "started_at", "status", "ingestion_runs",
                                       "succeeded", "skipped", "failed", "rows_written",
                                       "ingest_seconds")}
            for run in reversed(ordered[-RECENT_RUNS:])
        ],
    }
