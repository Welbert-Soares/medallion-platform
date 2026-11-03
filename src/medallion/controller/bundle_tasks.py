"""`medallion-bundle-tasks [--check]`: generate the job's dbt tasks inside `databricks.yml`.

One dbt task per Source System that has dbt models (`dbt/models/bronze/<system>/`), in the order
of `catalog_structure.yml`. Each runs `dbt deps`, then `dbt build --select tag:<system>`, so
bronze → silver → gold and their tests run in dependency order inside one task, and a failing Source
System never blocks another (ADR 0006). When the Source System's Raw sources declare a freshness
check, `dbt source freshness` runs last and fails the task if Raw stopped growing. Every task starts
after the Ingestions even when one failed (`ALL_DONE`).

A bundle cannot split one job's tasks across files, so the tasks live between two marker lines in
`databricks.yml`, rewritten by this command; `--check` fails when they are out of date.
"""

import argparse
import sys
from pathlib import Path

import yaml

from medallion.controller.config.loader import ConfigError, load_config
from medallion.controller.config.model import PlatformConfig

BEGIN = "        # BEGIN GENERATED dbt tasks (make bundle): do not edit by hand"
END = "        # END GENERATED dbt tasks"


def dbt_source_systems(config: PlatformConfig, models_dir: Path) -> list[str]:
    """Source Systems with bronze dbt models, in Catalog Structure order."""
    return [s.name for s in config.catalog_structure.source_systems
            if (models_dir / "bronze" / s.name).is_dir()]


def checks_freshness(models_dir: Path, system: str) -> bool:
    """Whether any Raw source of the Source System declares a freshness check."""
    sources_file = models_dir / "bronze" / system / "__sources.yml"
    if not sources_file.is_file():
        return False
    sources = (yaml.safe_load(sources_file.read_text()) or {}).get("sources", [])
    return any((source.get("config") or {}).get("freshness") or source.get("freshness")
               for source in sources)


def dbt_commands(system: str, freshness: bool) -> list[str]:
    commands = ["dbt deps", f"dbt build --select tag:{system}"]
    if freshness:
        commands.append(f"dbt source freshness --select source:{system}")
    return commands


def render_tasks(systems: list[str], bronze_catalog: str,
                 freshness: frozenset[str] = frozenset()) -> str:
    lines = [BEGIN]
    for system in systems:
        commands = ", ".join(f'"{c}"' for c in dbt_commands(system, system in freshness))
        lines += [
            f"        - task_key: dbt_{system}",
            "          depends_on:",
            "            - task_key: ingest",
            "          run_if: ALL_DONE",
            "          environment_key: dbt",
            "          dbt_task:",
            "            project_directory: ./dbt",
            f"            commands: [{commands}]",
            "            warehouse_id: ${var.warehouse_id}",
            f"            catalog: {bronze_catalog}",
            f"            schema: {system}",
        ]
    lines.append(END)
    return "\n".join(lines)


def replace_block(bundle_text: str, block: str) -> str:
    start, end = bundle_text.find(BEGIN), bundle_text.find(END)
    if start < 0 or end < start:
        raise ValueError("databricks.yml has no generated dbt tasks block (BEGIN/END markers)")
    return bundle_text[:start] + block + bundle_text[end + len(END):]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="medallion-bundle-tasks", description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="fail if databricks.yml is out of date instead of rewriting it")
    parser.add_argument("--config-dir", default="config/platform")
    parser.add_argument("--models-dir", default="dbt/models")
    parser.add_argument("--bundle", default="databricks.yml")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config_dir)
    except ConfigError as e:
        print(f"{args.config_dir}: {e}", file=sys.stderr)
        return 1

    models_dir = Path(args.models_dir)
    systems = dbt_source_systems(config, models_dir)
    freshness = frozenset(s for s in systems if checks_freshness(models_dir, s))
    block = render_tasks(systems, config.catalog_structure.layer("bronze").catalog, freshness)
    bundle = Path(args.bundle)
    current = bundle.read_text()
    updated = replace_block(current, block)

    if args.check:
        if updated != current:
            print(f"{bundle}: dbt tasks are out of date; run `make bundle`", file=sys.stderr)
            return 1
        print(f"{bundle}: dbt tasks up to date ({', '.join(systems)})")
        return 0

    bundle.write_text(updated)
    print(f"{bundle}: dbt tasks for {', '.join(systems)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
