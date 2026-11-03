"""metrics.json: platform shape, Run history and the external heartbeat (stale)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from medallion.controller.config.loader import load_config
from medallion.controller.metrics import (
    RECENT_RUNS,
    STALE_AFTER_HOURS,
    data_tables,
    dbt_counts,
    platform_metrics,
    row_count_sql,
    run_metrics_table,
    series_payload,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = load_config(REPO_ROOT / "config" / "platform")
NOW = datetime(2026, 11, 3, 10, 0, tzinfo=UTC)


def run(run_id: int, started: datetime, status: str = "success", rows: int = 24) -> dict:
    finished = started + timedelta(seconds=30)
    return {"run_id": run_id, "started_at": started.isoformat().replace("+00:00", "Z"),
            "finished_at": finished.isoformat().replace("+00:00", "Z"), "status": status,
            "ingestion_runs": 2, "succeeded": 1 if status == "success" else 0, "skipped": 1,
            "failed": 1 if status == "failed" else 0, "rows_written": rows,
            "ingest_seconds": 25.0}


def test_table_is_the_gold_platform_schema():
    assert run_metrics_table(CONFIG) == "medallion_gold.platform.run_metrics"


def test_platform_shape_comes_from_config():
    platform = platform_metrics(CONFIG, [], NOW)["platform"]

    assert platform["source_systems"] == 3
    assert platform["ingestions_active"] == sum(i.active for i in CONFIG.ingestions)
    assert platform["layers"] == ["raw", "bronze", "silver", "gold"]
    assert platform["dbt"] is None  # no manifest given


def test_run_totals_and_most_recent_first():
    runs = [run(1, NOW - timedelta(days=2)), run(3, NOW - timedelta(hours=4)),
            run(2, NOW - timedelta(days=1), status="failed", rows=0)]

    metrics = platform_metrics(CONFIG, runs, NOW)

    assert metrics["runs"]["total"] == 3
    assert metrics["runs"]["succeeded"] == 2
    assert metrics["runs"]["success_rate"] == 0.6667
    assert metrics["runs"]["rows_written"] == 48
    assert [r["run_id"] for r in metrics["recent_runs"]] == [3, 2, 1]
    assert "finished_at" not in metrics["recent_runs"][0]


def test_only_the_last_runs_are_listed_but_totals_count_all():
    runs = [run(i, NOW - timedelta(days=i)) for i in range(1, RECENT_RUNS + 6)]

    metrics = platform_metrics(CONFIG, runs, NOW)

    assert len(metrics["recent_runs"]) == RECENT_RUNS
    assert metrics["runs"]["total"] == RECENT_RUNS + 5


def test_a_recent_run_is_not_stale():
    runs = [run(1, NOW - timedelta(hours=4))]

    assert platform_metrics(CONFIG, runs, NOW)["runs"]["stale"] is False


def test_no_run_for_more_than_a_day_is_stale():
    """The job's own checks can't see the job not running; this one runs outside it."""
    runs = [run(1, NOW - timedelta(hours=STALE_AFTER_HOURS + 1))]

    metrics = platform_metrics(CONFIG, runs, NOW)

    assert metrics["runs"]["stale"] is True
    assert metrics["runs"]["hours_since_last_run"] > STALE_AFTER_HOURS


def test_no_runs_at_all_is_stale():
    metrics = platform_metrics(CONFIG, [], NOW)

    assert metrics["runs"]["stale"] is True
    assert metrics["runs"]["success_rate"] is None


def test_dbt_counts_ignore_installed_packages():
    manifest = {
        "metadata": {"project_name": "medallion"},
        "nodes": {
            "model.medallion.a": {"package_name": "medallion", "resource_type": "model"},
            "test.medallion.t1": {"package_name": "medallion", "resource_type": "test"},
            "test.medallion.t2": {"package_name": "medallion", "resource_type": "test"},
            "model.dbt_utils.x": {"package_name": "dbt_utils", "resource_type": "model"},
        },
        "sources": {"source.medallion.weather.sao_paulo": {"package_name": "medallion"}},
    }

    assert dbt_counts(manifest) == {"models": 1, "tests": 2, "sources": 1}


def _model(name, database, schema, alias, materialized, package="medallion"):
    return {"resource_type": "model", "package_name": package, "name": name,
            "database": database, "schema": schema, "alias": alias,
            "config": {"materialized": materialized}}


MANIFEST = {
    "metadata": {"project_name": "medallion"},
    "sources": {
        "source.medallion.weather.sao_paulo": {
            "package_name": "medallion", "name": "sao_paulo", "identifier": "sao_paulo",
            "database": "medallion_raw", "schema": "weather"},
        "source.medallion.controller.execution_log": {  # the Controller is not a Layer
            "package_name": "medallion", "name": "execution_log", "identifier": "execution_log",
            "database": "medallion_controller", "schema": "controller"},
    },
    "nodes": {
        "model.medallion.gold": _model("gold_weather__kpi_daily", "medallion_gold", "weather",
                                       "kpi_daily", "table"),
        "model.medallion.bronze": _model("bronze_weather__sao_paulo", "medallion_bronze",
                                         "weather", "sao_paulo", "incremental"),
        "model.medallion.stg": _model("stg_bronze_weather__sao_paulo", "medallion_bronze",
                                      "weather", "stg_sao_paulo", "view"),
        "model.dbt_utils.x": _model("x", "medallion_gold", "weather", "x", "table", "dbt_utils"),
        "test.medallion.t": {"resource_type": "test", "package_name": "medallion"},
    },
}


def test_data_tables_are_raw_sources_and_persisted_models_in_layer_order():
    tables = data_tables(CONFIG, MANIFEST)

    assert [(t["layer"], t["table"]) for t in tables] == [
        ("raw", "sao_paulo"), ("bronze", "sao_paulo"), ("gold", "kpi_daily")]
    assert tables[1]["relation"] == "medallion_bronze.weather.sao_paulo"
    assert data_tables(CONFIG, None) == []


def test_one_query_counts_every_table():
    sql = row_count_sql(data_tables(CONFIG, MANIFEST))

    assert sql.count("UNION ALL") == 2
    assert ("SELECT 'medallion_gold.weather.kpi_daily' AS relation, count(*) AS row_count "
            "FROM medallion_gold.weather.kpi_daily") in sql


def test_table_rows_are_published_without_the_relation():
    rows = [{**t, "rows": 10} for t in data_tables(CONFIG, MANIFEST)]

    tables = platform_metrics(CONFIG, [], NOW, MANIFEST, rows)["tables"]

    assert tables[0] == {"layer": "raw", "schema": "weather", "table": "sao_paulo", "rows": 10}


def test_series_payload_keeps_rows_as_they_are():
    rows = [{"reference_date": "2026-10-01", "avg_temperature": 21.5}]

    payload = series_payload("medallion_gold.weather.kpi_daily", rows, NOW)

    assert payload == {"generated_at": "2026-11-03T10:00:00+00:00",
                       "table": "medallion_gold.weather.kpi_daily", "rows": rows}
