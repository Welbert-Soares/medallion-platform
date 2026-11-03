"""metrics.json: platform shape, Run history and the external heartbeat (stale)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from medallion.controller.config.loader import load_config
from medallion.controller.metrics import (
    RECENT_RUNS,
    STALE_AFTER_HOURS,
    dbt_counts,
    platform_metrics,
    run_metrics_table,
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
