# One dbt project for all Layers, run per Source System by tag

All dbt models live in a single project (`dbt/`, named `medallion`) laid out as `models/<layer>/<source system>/`, and each Layer reads the previous one through `ref()`; only bronze reads Raw through `source()`. The job has one dbt task per Source System running `dbt build --select tag:<system>`, generated from `catalog_structure.yml` and the model folders by `medallion-bundle-tasks` (`make bundle`) and checked in CI. This replaces one project per Layer × Source System chained by `source()` and one job task per project.

We chose it because separate projects made the lineage stop at every Layer boundary (dbt docs showed islands, not raw → gold), and every new Source System cost three projects with their own `dbt_project.yml`, packages and job task written by hand. One task per Source System keeps the isolation we had: a failing Source System never blocks another, and inside a task `dbt build` runs models and tests in dependency order, so a failing silver test still stops that Source System's gold.

## Consequences

- Tables do not move: each Layer folder sets its catalog and each Source System folder its schema; `generate_schema_name` uses them verbatim and `generate_alias_name` drops the `<layer>_<system>__` prefix that keeps model names unique (`bronze_weather__sao_paulo` → table `sao_paulo`, `stg_silver_ibge__municipios` → view `stg_municipios`).
- A Layer folder must carry its Source System's `+tags` in `dbt_project.yml`, or the job never selects it; `tests/test_bundle_job.py` checks every folder.
- The job's dbt tasks live between generated markers in `databricks.yml`, because a bundle cannot split one job's tasks across files. Do not edit them by hand.
- Per-Layer tasks are gone from the job graph: a Run shows one dbt task per Source System, and which Layer failed is in that task's dbt output.
