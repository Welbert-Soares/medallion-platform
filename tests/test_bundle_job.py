"""Shape of the bundle job: one dbt task per Source System, isolated, running after Ingestions."""

from pathlib import Path

import yaml

from medallion.controller.bundle_tasks import main as bundle_tasks
from medallion.controller.config.loader import load_config

REPO_ROOT = Path(__file__).resolve().parents[1]
JOB = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())["resources"]["jobs"]["medallion"]
TASKS = {t["task_key"]: t for t in JOB["tasks"]}
DBT = REPO_ROOT / "dbt"
DBT_PROJECT = yaml.safe_load((DBT / "dbt_project.yml").read_text())
SYSTEMS_WITH_MODELS = sorted(p.name for p in (DBT / "models" / "bronze").iterdir() if p.is_dir())
STRUCTURE = load_config(REPO_ROOT / "config" / "platform").catalog_structure
# Model folders that are not Source Systems but schemas declared on a Layer (e.g. gold.platform).
LAYER_SCHEMAS = {(layer.name, schema.name) for layer in STRUCTURE.layers for schema in layer.schemas}


def upstream(task_key: str) -> list[str]:
    return [d["task_key"] for d in TASKS[task_key].get("depends_on", [])]


def test_generated_dbt_tasks_are_up_to_date(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)

    assert bundle_tasks(["--check"]) == 0, "run `make bundle`"


def test_every_source_system_with_models_has_one_dbt_task():
    dbt_tasks = {k: t for k, t in TASKS.items()
                 if "dbt_task" in t and k.removeprefix("dbt_") in SYSTEMS_WITH_MODELS}

    assert sorted(dbt_tasks) == [f"dbt_{s}" for s in SYSTEMS_WITH_MODELS]
    for key, task in dbt_tasks.items():
        system = key.removeprefix("dbt_")
        # The single project at the root of dbt/, whose name ('medallion') names the profile
        # Databricks generates: no --project-dir or --profile needed.
        assert task["dbt_task"]["project_directory"] == "./dbt"
        commands = task["dbt_task"]["commands"]
        assert commands[:2] == ["dbt deps", f"dbt build --select tag:{system}"]
        assert commands[2:] in ([], [f"dbt source freshness --select source:{system}"])
        assert task["dbt_task"]["warehouse_id"] == "${var.warehouse_id}"


def test_dbt_runs_after_ingestions_even_when_one_failed():
    for system in SYSTEMS_WITH_MODELS:
        assert upstream(f"dbt_{system}") == ["ingest"]
        assert TASKS[f"dbt_{system}"]["run_if"] == "ALL_DONE"


def test_dbt_project_names_the_generated_profile():
    assert DBT_PROJECT["name"] == DBT_PROJECT["profile"] == "medallion"


def test_every_model_folder_is_tagged_and_scheduled():
    """Tasks select by tag: a folder without the tag, or without a task, would never run."""
    models = DBT_PROJECT["models"]["medallion"]
    for layer_dir in (DBT / "models").iterdir():
        for folder in (p for p in layer_dir.iterdir() if p.is_dir()):
            where = f"{layer_dir.name}/{folder.name}"
            config = models[layer_dir.name][folder.name]
            assert config["+tags"] == [folder.name], where
            assert config["+schema"] == folder.name, where
            assert (folder.name in SYSTEMS_WITH_MODELS
                    or (layer_dir.name, folder.name) in LAYER_SCHEMAS), (
                f"{where}: neither a Source System with bronze models nor a declared Layer schema")
            assert f"dbt build --select tag:{folder.name}" in TASKS[f"dbt_{folder.name}"][
                "dbt_task"]["commands"], where


def test_platform_metrics_are_built_after_every_ingest_even_a_failed_one():
    task = TASKS["dbt_platform"]

    assert ("gold", "platform") in LAYER_SCHEMAS
    assert upstream("dbt_platform") == ["ingest"]
    assert task["run_if"] == "ALL_DONE"
    assert task["dbt_task"]["commands"] == ["dbt deps", "dbt build --select tag:platform"]


def test_freshness_runs_only_where_raw_declares_it():
    """weather lands daily; ibge is a static CSV that only lands when the file changes."""
    assert TASKS["dbt_weather"]["dbt_task"]["commands"][-1] == (
        "dbt source freshness --select source:weather")
    assert not any("freshness" in c for c in TASKS["dbt_ibge"]["dbt_task"]["commands"])


def test_failed_ingestion_fails_the_run_through_a_leaf_task():
    leaf = TASKS["ingestions_succeeded"]

    assert upstream("ingestions_succeeded") == ["ingest"]
    assert "run_if" not in leaf
    assert not [t for t in TASKS.values() if "ingestions_succeeded" in
                [d["task_key"] for d in t.get("depends_on", [])]]


def test_job_runs_daily_at_six_in_sao_paulo():
    assert JOB["schedule"] == {"quartz_cron_expression": "0 0 6 * * ?",
                               "timezone_id": "America/Sao_Paulo", "pause_status": "UNPAUSED"}
    assert JOB["max_concurrent_runs"] == 1
