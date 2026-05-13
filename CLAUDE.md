# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Metadata-driven Medallion pipeline on Databricks. Config lives in versioned YAML files (`config/controller/*.yml`) — the source of truth. A sync notebook reads those files and upserts into the Delta table `cli_catalog.controller.ingestions`. At runtime, notebooks read from that table.

## Catalog and schemas

All objects live in the Unity Catalog **`cli_catalog`**:

| Schema | Purpose |
|---|---|
| `controller` | `ingestions` config table + `execution_log` |
| `bronze` | Raw Delta tables written by `generic_ingestion.py` |
| `silver` | dbt incremental models (cleaned, typed, deduplicated) |
| `gold` | dbt table models (KPIs, aggregations) |

## Commands

```bash
make setup       # pip install -r requirements.txt + dbt deps + copy profiles template
make compile     # dbt compile — validates SQL without executing
make test        # dbt compile + dbt test (requires live Databricks connection)
make docs        # dbt docs generate + serve
make upload      # push notebooks, config/, dbt project and CSV sources to Databricks
make run         # trigger the full Databricks job via CLI
make clean       # dbt clean
```

Lint Python notebooks: `ruff check databricks/notebooks/`

## Config architecture (YAML → Delta)

```
config/controller/*.yml       ← source of truth (Git-versioned)
        ↓  (sync_config.py reads + validates + upserts)
cli_catalog.controller.ingestions   ← runtime config (Delta table)
        ↓  (controller.py reads active rows)
generic_ingestion.py          ← executes one ingestion per row
```

Adding a new source: create a new YAML file in `config/controller/`. No Python code needed.

## YAML structure

Each YAML file has one top-level key `ingestions` with a list of source groups. Each group has:
- `name` — grouper (e.g., `weather`, `ibge`)
- `source.type` — `api | csv | sql`
- `source.connection_id` — references `connections.yml` (sql only, Phase 3)
- `target.container` / `target.schema` — destination schema in `cli_catalog`
- `tables[]` — list of tables, each with `name`, `name_target`, `type_load`, `params`, `active`, and source-specific keys

Source-specific keys:
- **api**: `url.link`, `url.mode` (`fixed|template_range|query_params`), `url.params` (dict — serialized to JSON), `url.flatten_method` (`zip_arrays|default`), `url.data_key`, `url.watermark_column`
- **csv**: `csv.path` (DBFS), `csv.delimiter`, `csv.header`
- **sql**: `sql.query` (use `?` as watermark placeholder), `sql.filter`, `sql.watermark_column`

String values in YAML support macros: `{{ today }}`, `{{ yesterday }}`, `{{ current_year }}`, `{{ last_year }}`.

All YAML files are validated against `config/controller/schemas/ingestions.yml` using `cerberus` before the upsert.

## Delta table: `controller.ingestions` columns

The table mirrors the flattened YAML structure. Key columns:

| Column | Source |
|---|---|
| `ingestion_name` (PK) | `tables[].name` |
| `source_name` | `name` (grouper) |
| `source_type` | `source.type` |
| `table_name_target` | `tables[].name_target` (always lowercase) |
| `table_type_load` | `tables[].type_load` |
| `table_active` | `tables[].active` |
| `table_url_params` | `tables[].url.params` serialized as JSON |
| `table_url_last_watermark` | updated by `generic_ingestion.py` after each API run |
| `table_sql_last_watermark` | updated by `generic_ingestion.py` after each SQL run |
| `sync_at` | timestamp of last YAML sync |

## Job flow (`main_workflow.json`)

```
sync_config → ingest_bronze → dbt_silver → dbt_gold
```

`sync_config` runs first on every execution, keeping the Delta table in sync with the YAMLs in DBFS. Scheduled daily at 06:00 BRT.

## Python utils (`config/utils/`)

| File | Purpose |
|---|---|
| `extract_controller.py` | Loads YAML, flattens nested dicts, returns `list[dict]` |
| `yaml_schema_validator.py` | Validates YAML content via cerberus + schema file |
| `macro_resolver.py` | Resolves `{{ macro }}` placeholders in string values |

`_flatten_dict` recursively flattens nested dicts with `_` separator. Exception: keys in `_JSON_SERIALIZE_KEYS` (currently `{"params"}`) are serialized to JSON string instead of expanded into individual columns.

## dbt conventions

- Silver models: `incremental` + `merge` strategy with `unique_key`; filter `WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})`.
- Gold models: `table` materialization; only reference `{{ ref('silver_...') }}`, never Bronze sources.
- `generate_schema_name.sql` macro strips the default prefix so `+schema: silver` resolves to `cli_catalog.silver` (not `cli_catalog.silver_silver`).
- Bronze tables always have `_ingested_at` (TIMESTAMP) and `_ingestion_name` (STRING).

## Deploy flow (`deploy.yml`)

On merge to `main`:
1. `databricks repos update $DATABRICKS_REPO_ID --branch main` — pull latest code
2. Upload CSV sources → `cli_catalog.bronze.sources` Unity Catalog Volume via SDK
3. Upsert job by name using `main_workflow.json` (warehouse path is already set in the file)
4. Trigger job run

Code (notebooks, config YAML, dbt models) is served directly from the Databricks Repo path:
`/Workspace/Repos/welbertsoares6@gmail.com/medallion-platform/`

## GitHub Secrets required

`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_REPO_ID`

The SQL Warehouse HTTP path (`/sql/1.0/warehouses/12ca72db06431bb2`) is committed directly in `main_workflow.json` — no secret needed.

## Active demo sources

| `ingestion_name` | YAML file | Type | Target table | Active |
|---|---|---|---|---|
| `weather_sao_paulo` | `weather.yml` | API (Open Meteo) | `bronze.weather_raw` | yes |
| `ibge_municipios` | `ibge.yml` | CSV (DBFS) | `bronze.ibge_municipios_raw` | yes |
| `orders_postgres` | `orders.yml` | SQL (JDBC) | `bronze.orders_raw` | no |

## Phases

| Phase | Status | What was delivered |
|---|---|---|
| 1 — YAML config + sync | **Done** | YAML files, `config/utils/`, `sync_config.py`, Delta schema |
| 2 — `delta_utils.py` | **Done** | `write_to_delta()`, `normalize_column_names()` in `config/utils/delta_utils.py` |
| 3 — Connections | **Done** | `connections.yml`, `connection_functions.py`, `orders_postgres` source (inactive) |
| 4 — CI/CD | **Done** | `ci.yml` (lint + YAML validation + dbt compile), `deploy.yml` (repos update + SDK upsert) |
