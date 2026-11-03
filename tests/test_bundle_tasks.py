"""Generating the job's dbt tasks from the Catalog Structure and the dbt model folders."""

from pathlib import Path

import pytest

from medallion.controller.bundle_tasks import (
    BEGIN,
    END,
    dbt_source_systems,
    main,
    render_tasks,
    replace_block,
)
from medallion.controller.config.loader import load_config

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = load_config(REPO_ROOT / "config" / "platform")


def test_only_source_systems_with_bronze_models_get_a_task(tmp_path):
    (tmp_path / "bronze" / "weather").mkdir(parents=True)
    (tmp_path / "silver" / "ibge").mkdir(parents=True)  # no bronze: nothing to start from

    assert dbt_source_systems(CONFIG, tmp_path) == ["weather"]


def test_tasks_follow_catalog_structure_order(tmp_path):
    for system in ("ibge", "orders", "weather"):
        (tmp_path / "bronze" / system).mkdir(parents=True)

    declared = [s.name for s in CONFIG.catalog_structure.source_systems]
    assert dbt_source_systems(CONFIG, tmp_path) == declared


def test_each_task_builds_its_source_system_by_tag():
    block = render_tasks(["weather"], "medallion_bronze")

    assert block.startswith(BEGIN) and block.endswith(END)
    assert "        - task_key: dbt_weather" in block
    assert '            commands: ["dbt build --select tag:weather"]' in block
    assert "          run_if: ALL_DONE" in block


def test_replace_block_keeps_everything_outside_the_markers():
    text = f"before\n{BEGIN}\n        - task_key: stale\n{END}\nafter\n"

    updated = replace_block(text, render_tasks(["ibge"], "medallion_bronze"))

    assert updated.startswith(f"before\n{BEGIN}\n") and updated.endswith(f"{END}\nafter\n")
    assert "stale" not in updated and "dbt_ibge" in updated


def test_replace_block_needs_the_markers():
    with pytest.raises(ValueError, match="BEGIN/END markers"):
        replace_block("no markers here", render_tasks([], "medallion_bronze"))


def test_check_fails_on_a_stale_bundle_and_write_fixes_it(tmp_path, capsys):
    bundle = tmp_path / "databricks.yml"
    bundle.write_text(f"jobs:\n{BEGIN}\n{END}\n")
    models = tmp_path / "models"
    (models / "bronze" / "weather").mkdir(parents=True)
    args = ["--config-dir", str(REPO_ROOT / "config" / "platform"),
            "--models-dir", str(models), "--bundle", str(bundle)]

    assert main([*args, "--check"]) == 1
    assert "out of date" in capsys.readouterr().err

    assert main(args) == 0
    assert main([*args, "--check"]) == 0
    assert "dbt_weather" in bundle.read_text()
