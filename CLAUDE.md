# CLAUDE.md

Metadata-driven Medallion platform on Databricks. Read `CONTEXT.md` before naming anything (Source
System, Ingestion, Connection, Watermark, Ingestion State, Catalog Structure, Controller, Layer, Raw,
Run, Ingestion Run) and the ADRs in `docs/adr/` before changing an area they cover. `README.md` has
the overview, the job diagram and the `make` targets.

## Layout

- `src/medallion/` — the wheel, hexagonal (ADR 0005):
  - `controller/` — config (`load_config`, Pydantic models), Catalog Structure `plan`/`apply`,
    config sync, runtime ports (`IngestionCatalog`, `RunLog`, `ConnectionCatalog`, `SecretProvider`,
    `Clock`).
  - `ingestion/` — `run_ingestion` use case, `SourceReader`/`RawWriter` ports, API window and
    request building, JDBC query building.
  - `databricks/` — every adapter touching Spark, Delta, the SDK or dbutils, plus the job task and
    CLI entry points (composition roots).
- `config/platform/` — the only config: `catalog_structure.yml`, `connections.yml`,
  `ingestions/<source_system>.yml`.
- `dbt/` — one dbt project per Layer × Source System (`bronze_ibge`, `silver_weather`, …) plus the
  shared `utils` package.
- `databricks.yml` — bundle: wheel artifact and the `medallion` job.
- `tests/` — pytest; in-memory port fakes in `tests/fakes/`.

## Rules that are easy to break

- **Import boundaries** (ADR 0005, enforced by ruff `TID251`): only `medallion.databricks` imports
  pyspark, databricks-sdk, delta or dbutils; `medallion.controller` never imports
  `medallion.ingestion`. Business rules go in `controller`/`ingestion` so they stay testable.
- **Tests stay Spark-free**: they drive the use cases (`load_config`, `run_ingestion`, Catalog
  Structure `plan`/`apply`, `sync_config`) through `tests/fakes/`, passing lists of dicts where a
  DataFrame would flow. Adapters are thin and verified by a Run in the workspace, not unit tests.
- **Raw is append-only** (ADR 0002): every load is appended and stamped `_run_id` / `_ingested_at`.
  Each bronze staging model deduplicates with
  `qualify row_number() over (partition by <key> order by _ingested_at desc) = 1`, or its incremental
  MERGE fails on duplicate keys.
- **Config and state are separate tables** (ADR 0003): `ingestions` / `connections` are overwritten
  by every sync; Watermarks live only in `ingestion_state`, written by `run_ingestion` after Raw was
  written.
- **Targets are derived**: an Ingestion lands in `{raw catalog}.{source_system}.{raw_table}`; never
  add catalog/schema fields to an Ingestion.
- **Catalog Structure owns grants** (ADR 0004): apply revokes any grant on managed catalogs/schemas
  that YAML does not declare, and never drops objects (it reports orphans). Declare grants in
  `catalog_structure.yml`; run `make plan` before changing it.
- **Secrets**: a Connection holds only `secret_scope` / `secret_key`. Passwords are put by the
  maintainer by hand (`docs/connections.md`); never write one to YAML, tables, logs or chat.
- **New config field** → add it to the Pydantic model and, for Ingestion sources, a column in
  `INGESTION_COLUMNS` (`controller/sync.py`); the sync fails loudly when they drift.

## Controller tables (`medallion_controller.controller`)

| Table | Written by | Holds |
|---|---|---|
| `ingestions` | `sync_config` (overwrite) | YAML mirror, sources flattened as `source_*` |
| `connections` | `sync_config` (overwrite) | address + secret reference |
| `ingestion_state` | `run_ingestion` (MERGE) | Watermark (column value, window end date or `sha256:` file hash), last Run |
| `execution_log` | `run_ingestion` (append) | one row per Ingestion Run: `success` / `skipped` / `failed` |

## Job and deploy

- Tasks: `sync_config` (also publishes the `active_ingestions` task value) → `ingest` for-each
  calling `medallion-run-ingestion` per active Ingestion → one `dbt_task` per dbt project.
  Bronze runs with `run_if: ALL_DONE`; the leaf `ingestions_succeeded` makes the Run fail when any
  Ingestion failed. Adding a dbt project means adding its task (`tests/test_bundle_job.py` checks).
- dbt tasks: the `dbt_task` copies `project_directory` to `/tmp` before running, so every task
  points at the whole `./dbt` folder (the shared `../utils` package must travel with it) and picks
  its project with `--project-dir <project>`. Databricks names the profile it generates after the
  root `dbt/dbt_project.yml` (`medallion`), so connecting commands pass `--profile medallion`.
  Local `dbt parse` and CI cannot catch breakage here; only a Run does.
- Deploy (`.github/workflows/deploy.yml`, on merge to `main`): Catalog Structure apply → secret
  scopes → CSV upload to `/Volumes/medallion_raw/sources/files/` → `bundle deploy`. No Run is
  triggered. GitHub environment `prod` holds `DATABRICKS_HOST` / `DATABRICKS_TOKEN`.
- CI (PRs to `main` only): ruff, pytest, wheel build, config validation, `dbt parse` of every
  project, `bundle validate`, Catalog Structure dry-run.

## dbt conventions

- `models/sources/__sources_*.yml`, `models/staging/stg_*` views (casting/renaming, dedup in
  bronze), `models/marts/*` incremental merge via `utils.incremental_join_where(ref(...), key)`,
  which adds `etl_created_date` / `etl_updated_date`.
- Raw tracking columns are `_run_id` and `_ingested_at` only; tables written before the cutover may
  still carry a legacy `_ingestion_name`, which models must not select.
- Bronze reads `source('{system}', ...)` from raw; silver reads `source('bronze_{system}', ...)`;
  gold reads `source('silver_{system}', ...)`.

## Agent skills

### Issue tracker

Issues e specs ficam no GitHub Issues de Welbert-Soares/medallion-platform (via `gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Labels canônicos padrão: needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: um `CONTEXT.md` + `docs/adr/` na raiz. See `docs/agents/domain.md`.
