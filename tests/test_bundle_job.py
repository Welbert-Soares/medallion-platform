"""Shape of the bundle job: dbt chains per Source System, isolated, running after Ingestions."""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
JOB = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())["resources"]["jobs"]["medallion"]
TASKS = {t["task_key"]: t for t in JOB["tasks"]}
DBT_PROJECTS = sorted(p.parent.name for p in (REPO_ROOT / "dbt").glob("*/dbt_project.yml")
                      if p.parent.name != "utils")


def upstream(task_key: str) -> list[str]:
    return [d["task_key"] for d in TASKS[task_key].get("depends_on", [])]


def test_every_dbt_project_has_a_native_dbt_task():
    dbt_tasks = {k: t for k, t in TASKS.items() if "dbt_task" in t}

    assert sorted(dbt_tasks) == [f"dbt_{p}" for p in DBT_PROJECTS]
    for key, task in dbt_tasks.items():
        project = key.removeprefix("dbt_")
        # The whole dbt/ folder, so the shared ../utils package is copied along with the project.
        assert task["dbt_task"]["project_directory"] == "./dbt"
        assert task["dbt_task"]["commands"] == [
            f"dbt deps --project-dir {project}",
            f"dbt build --project-dir {project} --profile medallion"]
        assert task["dbt_task"]["warehouse_id"] == "${var.warehouse_id}"


def test_root_dbt_project_names_the_generated_profile():
    root = yaml.safe_load((REPO_ROOT / "dbt" / "dbt_project.yml").read_text())

    assert root["profile"] == "medallion"


def test_bronze_runs_after_ingestions_even_when_one_failed():
    for project in DBT_PROJECTS:
        if project.startswith("bronze_"):
            assert upstream(f"dbt_{project}") == ["ingest"]
            assert TASKS[f"dbt_{project}"]["run_if"] == "ALL_DONE"


def test_layers_chain_within_each_source_system_only():
    for project in DBT_PROJECTS:
        layer, system = project.split("_", 1)
        if layer == "silver":
            assert upstream(f"dbt_{project}") == [f"dbt_bronze_{system}"]
        if layer == "gold":
            assert upstream(f"dbt_{project}") == [f"dbt_silver_{system}"]
        if layer != "bronze":
            assert "run_if" not in TASKS[f"dbt_{project}"]  # a failed upstream layer stops here


def test_failed_ingestion_fails_the_run_through_a_leaf_task():
    leaf = TASKS["ingestions_succeeded"]

    assert upstream("ingestions_succeeded") == ["ingest"]
    assert "run_if" not in leaf
    assert not [t for t in TASKS.values() if "ingestions_succeeded" in
                [d["task_key"] for d in t.get("depends_on", [])]]
