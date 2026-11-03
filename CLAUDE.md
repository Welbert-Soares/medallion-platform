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
- `dbt/` — one dbt project (`medallion`, ADR 0006): `models/<layer>/<source system>/{staging,marts}/`,
  shared macros in `dbt/macros/`.
- `databricks.yml` — bundle: wheel artifact and the `medallion` job. Its dbt tasks sit between
  generated markers: regenerate with `make bundle` (`medallion-bundle-tasks`), never by hand.
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
  calling `medallion-run-ingestion` per active Ingestion → one `dbt_<system>` task per Source
  System with dbt models, running `dbt deps`, `dbt build --select tag:<system>` (bronze → silver →
  gold, tests in between) and, if its Raw sources declare `freshness`, `dbt source freshness
  --select source:<system>` last. Each runs with `run_if: ALL_DONE`; the leaf `ingestions_succeeded` makes the Run fail
  when any Ingestion failed. `tests/test_bundle_job.py` checks the generated tasks are up to date.
- dbt tasks: `project_directory: ./dbt` is the project itself; Databricks names the profile it
  generates after it (`medallion`), so no `--project-dir` / `--profile` flags. Local `dbt parse` and
  CI cannot catch connection or runtime breakage here; only a Run does.
- Deploy (`.github/workflows/deploy.yml`, on merge to `main`): Catalog Structure apply → secret
  scopes → CSV upload to `/Volumes/medallion_raw/sources/files/` → `bundle deploy`; then `docs`
  (`dbt docs generate --static` against the warehouse) → `publish-docs` (GitHub Pages, its own
  `github-pages` environment with only `pages`/`id-token` write). No Run is triggered. GitHub environment `prod` holds `DATABRICKS_HOST` / `DATABRICKS_TOKEN`.
- CI (PRs to `main` only): ruff, pytest, wheel build, config validation, `medallion-bundle-tasks
  --check`, `dbt parse` of the project, `bundle validate`, Catalog Structure dry-run.

## dbt conventions

- Per `models/<layer>/<system>/`: `staging/stg_<layer>_<system>__<table>` views (casting/renaming,
  dedup in bronze) and `marts/<layer>_<system>__<table>`, which set `materialized` in `config()`
  (incremental merge via `incremental_join_where(ref(...), key)`, adding `etl_created_date` /
  `etl_updated_date`, or `table`). Bronze also holds `__sources.yml` for its Raw tables.
- Model names are unique project-wide; `generate_alias_name` keeps the table name after `__`
  (`bronze_weather__sao_paulo` → `sao_paulo`, staging → `stg_sao_paulo`), so tables never move.
- Every `<layer>/<system>` folder needs `+schema: <system>` and `+tags: ["<system>"]` in
  `dbt_project.yml`; the Layer sets `+database`. Without the tag the job never runs it (a test
  checks).
- Raw tracking columns are `_run_id` and `_ingested_at` only; tables written before the cutover may
  still carry a legacy `_ingestion_name`, which models must not select.
- Bronze reads `source('{system}', ...)` from raw; silver and gold read the previous Layer with
  `ref('<layer>_<system>__<table>')`, never `source()`, so lineage is one graph.
- Data tests: `unique` + `not_null` on every mart key, `not_null` on measures, `dbt_utils.
  accepted_range` / `accepted_values` for plausible values. Generic test args go under
  `arguments:` (dbt ≥1.10 syntax). Check new tests against real data before adding them.
- Gold marts are `contract: enforced`: every column has `data_type` (+ `not_null` constraints) and
  the SQL casts to those types explicitly.
- Freshness lives in `config:` of a bronze `__sources.yml` (`loaded_at_field: _ingested_at`); only
  for sources that land every Run. Static reference files (ibge) have none.
- `dbt_utils` is pinned by `dbt/package-lock.yml`; `dbt deps` before parse/build.
- Local runs: `dbt/` may hold a personal, git-ignored `profiles.yml`; dbt reads it before
  `~/.dbt`, so pass `--profiles-dir` when using another profile.

## Agent skills

### Issue tracker

Issues e specs ficam no GitHub Issues de Welbert-Soares/medallion-platform (via `gh` CLI). See `docs/agents/issue-tracker.md`.

### Triage labels

Labels canônicos padrão: needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: um `CONTEXT.md` + `docs/adr/` na raiz. See `docs/agents/domain.md`.
