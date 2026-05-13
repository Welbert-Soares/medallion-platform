# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Metadata-driven Medallion pipeline on Databricks. Config lives in versioned YAML files (`config/controller/*.yml`) — the source of truth. A sync notebook reads those files and upserts into the Delta table `medallion_controller.delta_controller.ingestions`. At runtime, notebooks read from that table and run ingestion into `medallion_raw`. dbt projects then transform the data through bronze → silver → gold.

## Catalog architecture

Five Unity Catalog catalogs — created manually, managed via code:

| Catalog | Purpose |
|---|---|
| `medallion_controller` | Config table `ingestions` + `execution_log` |
| `medallion_raw` | Landing zone — data as-is from sources (no transformations) |
| `medallion_bronze` | First medallion layer — standardized and typed (dbt) |
| `medallion_silver` | Cleaned, deduplicated, business-typed (dbt) |
| `medallion_gold` | KPIs and analytics-ready models (dbt) |

Schemas inside each catalog follow the **source system / domain** name (e.g., `weather`, `ibge`, `orders`).

Full path pattern: `{catalog}.{system}.{table}` — e.g., `medallion_raw.weather.sao_paulo`.

## Controller schema: `medallion_controller.delta_controller`

| Table | Purpose |
|---|---|
| `ingestions` | Runtime config — mirrors YAML files |
| `execution_log` | Log of every ingestion run |

## Commands

```bash
# Per dbt project (run from inside dbt/<project>/)
dbt deps                          # install packages (utils)
dbt compile --profiles-dir ~/.dbt # validate SQL
dbt run    --profiles-dir ~/.dbt  # execute models
dbt test   --profiles-dir ~/.dbt  # run data tests

# Lint Python
ruff check databricks/notebooks/ config/utils/ scripts/
```

## Config architecture (YAML → Delta → Raw)

```
config/controller/*.yml             ← source of truth (Git-versioned)
        ↓  (sync_config.py validates + upserts)
medallion_controller.delta_controller.ingestions
        ↓  (controller.py reads active rows, runs in parallel threads)
generic_ingestion.py                ← one ingestion per row → medallion_raw.{system}.{table}
```

Adding a new source: create a YAML file in `config/controller/`. No Python code needed.

## YAML structure

Each YAML file has top-level key `ingestions` with a list of source groups:
- `name` — system name (e.g., `weather`, `ibge`)
- `source.type` — `api | csv | sql | table`
- `source.connection_id` — references `connections.yml` (sql/table only)
- `source.schema` — source DB schema (table type only, e.g., `dbo`)
- `target.container` — destination catalog (e.g., `medallion_raw`)
- `target.schema` — destination schema = system name (e.g., `weather`)

Source-specific keys:
- **api**: `url.link`, `url.mode`, `url.params` (JSON), `url.flatten_method` (`zip_arrays|json_normalize|default`), `url.data_key`, `url.watermark_column`
- **csv**: `csv.path` (Volume path), `csv.delimiter`, `csv.header`
- **sql/table**: `sql.query`, `sql.filter`, `sql.watermark_column`, `sql.partition_column`

Table params: `flag_distinct`, `flag_hash_row`, `flag_data_factory` (all boolean).

String values support macros: `{{ today }}`, `{{ yesterday }}`, `{{ current_year }}`, `{{ last_year }}`.

Schema validated via `cerberus` against `config/controller/schemas/ingestions.yml`.

## Active sources

| `ingestion_name` | YAML | Type | Target | Active |
|---|---|---|---|---|
| `weather_sao_paulo` | `weather.yml` | API (Open Meteo) | `medallion_raw.weather.sao_paulo` | yes |
| `ibge_municipios` | `ibge.yml` | CSV (Volume) | `medallion_raw.ibge.municipios` | yes |
| `orders_postgres` | `orders.yml` | SQL (JDBC) | `medallion_raw.orders.orders` | no |

CSV volume path: `/Volumes/medallion_raw/sources/files/`

## dbt multi-project architecture

Each source system and layer is a **separate dbt project** inside `dbt/`:

```
dbt/
├── utils/                  ← shared macros package (local dependency)
├── bronze_weather/         ← raw → bronze.weather
├── bronze_ibge/            ← raw → bronze.ibge
├── silver_weather/         ← bronze.weather → silver.weather
├── silver_ibge/            ← bronze.ibge → silver.ibge
└── gold_weather/           ← silver.weather → gold.weather
```

### Internal structure of each project

```
{project}/
├── dbt_project.yml         ← profile = project name, catalog+schema fixed via +database/+schema
├── packages.yml            ← depends on local utils package
└── models/
    ├── sources/            ← __sources_{layer}_{system}.yml
    ├── staging/            ← stg_* views (light casting/renaming only)
    └── marts/              ← final Delta tables (incremental merge or table)
```

### dbt_project.yml pattern (YAML anchors)

```yaml
name: &project_name 'bronze_weather'
profile: *project_name
models:
  +databricks_tags: &project_tags
    project: *project_name
  bronze_weather:
    +database: medallion_bronze
    +schema: weather
    staging:
      +materialized: view
    marts:
      +materialized: incremental
      +incremental_strategy: merge
```

### Conventions

| Convention | Description |
|---|---|
| `stg_*` | Staging views — light type casting, renaming, nullif only |
| `marts/*` | Final tables — business logic, incremental merge |
| `__sources_*.yml` | Source definitions — double underscore sorts first |
| `etl_created_date` | Timestamp of first insert (excluded from merge) |
| `etl_updated_date` | Timestamp of last update |
| `pk_*` / `fk_*` | Primary / foreign key columns |

### Key macro: `utils.incremental_join_where`

Handles incremental merge with change detection:
- Compares source vs target columns (null-safe `<=>`)
- Only upserts rows that are new or changed
- Adds `etl_created_date` / `etl_updated_date` automatically
- `merge_exclude_columns=['etl_created_date']` prevents overwriting creation date

```sql
{{ utils.incremental_join_where(ref('stg_model'), 'unique_key_column') }}
```

### Source naming

- Bronze sources raw: `source('{system}', '{table}')` → `medallion_raw.{system}.{table}`
- Silver sources bronze: `source('bronze_{system}', '{table}')` → `medallion_bronze.{system}.{table}`
- Gold sources silver: `source('silver_{system}', '{table}')` → `medallion_silver.{system}.{table}`

### profiles.yml (local `~/.dbt/profiles.yml`)

One profile per project, profile name = project name:

```yaml
bronze_weather:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/12ca72db06431bb2
      token: <pat>
      catalog: medallion_bronze
      schema: weather
      threads: 4
```

## Job flow (`main_workflow.json`)

```
sync_config → ingest_raw → dbt_silver → dbt_gold
```

Scheduled daily at 06:00 BRT. The dbt notebooks (`run_silver.py`, `run_gold.py`) generate the profile at runtime from the notebook context (host + token) and run each project in sequence.

## Deploy flow (`deploy.yml`)

On merge to `main`:
1. Update Databricks Repo (`repos update --branch main`)
2. Ensure schemas, tables and volumes exist (idempotent DDL via SDK)
3. Upload CSV sources → `/Volumes/medallion_raw/sources/files/`
4. Upsert job by name using `main_workflow.json`
5. Trigger job run

GitHub Environment: `teste` (secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_REPO_ID`)

Warehouse path (`/sql/1.0/warehouses/12ca72db06431bb2`) committed directly in `main_workflow.json`.

## Python utils (`config/utils/`)

| File | Purpose |
|---|---|
| `extract_controller.py` | Loads YAML, flattens nested dicts, returns `list[dict]` |
| `yaml_schema_validator.py` | Validates via cerberus + schema file |
| `macro_resolver.py` | Resolves `{{ macro }}` placeholders in strings |
| `delta_utils.py` | `write_to_delta()`, `normalize_column_names()` |
| `connection_functions.py` | `get_jdbc_reader()` for SQL/table sources |

`_flatten_dict` flattens nested dicts with `_` separator. Keys in `_JSON_SERIALIZE_KEYS` (`{"params"}`) are serialized to JSON string instead.

## Phases

| Phase | Status | What was delivered |
|---|---|---|
| 1 — YAML config + sync | **Done** | YAML files, `config/utils/`, `sync_config.py`, Delta schema |
| 2 — `delta_utils.py` | **Done** | `write_to_delta()`, `normalize_column_names()` |
| 3 — Connections | **Done** | `connections.yml`, `connection_functions.py`, `orders_postgres` (inactive) |
| 4 — CI/CD | **Done** | `ci.yml` (lint + YAML validation), `deploy.yml` (repos update + SDK upsert) |
| 5 — Multi-catalog architecture | **Done** | 5 catalogs `medallion_*`, schemas por sistema, raw separado do medallion |
| 6 — dbt multi-project | **Done** | `utils/` + `bronze_*/silver_*/gold_*` projects, macro `incremental_join_where` |
