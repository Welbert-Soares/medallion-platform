# Medallion Platform

Metadata-driven Medallion pipeline on Databricks. Add a new data source by creating a YAML file — no Python required.

## Architecture

```
config/controller/*.yml            ← source of truth (Git-versioned)
         ↓  sync_config notebook
cli_catalog.controller.ingestions  ← runtime config (Delta table)
         ↓  controller notebook (parallel tasks)
generic_ingestion notebook         ← Bronze: raw data per source
         ↓  dbt Core
cli_catalog.silver.*               ← cleaned, typed, deduplicated
         ↓  dbt Core
cli_catalog.gold.*                 ← KPIs and analytics-ready models
```

**Daily job** (`medallion-platform-controller`): `sync_config → ingest_bronze → dbt_silver → dbt_gold`, scheduled 06:00 BRT.

## First-time setup

1. Run the `00_create_tables` notebook in Databricks once to create schemas, tables, and the CSV Volume.
2. Add the GitHub Secrets below to your fork.
3. Push to `main` — GitHub Actions deploys and runs the pipeline.

## Local development

```bash
make setup      # pip install + copy dbt profiles template
make compile    # dbt compile — validates SQL without executing
make test       # dbt compile + dbt test (requires live Databricks connection)
make docs       # dbt docs generate + serve
make clean      # dbt clean
```

Lint: `ruff check databricks/notebooks/ config/utils/ scripts/`

Validate YAML configs: `python scripts/validate_configs.py`

## Adding a new source

Create `config/controller/<source>.yml`. The next deploy syncs it to the Delta table automatically.

```yaml
ingestions:
  - name: my_source
    source:
      type: api          # api | csv | sql
    target:
      container: bronze
      schema: bronze
    tables:
      - name: my_table_ingestion
        name_target: my_table_raw
        type_load: append  # append | overwrite | merge | replace_where
        active: true
        url:
          link: "https://api.example.com/data"
          mode: fixed      # fixed | template_range | query_params
```

See `config/controller/weather.yml` (API) and `config/controller/ibge.yml` (CSV) for complete examples.

## Deploy (GitHub Actions)

Push to `main` → `deploy.yml` runs automatically:

1. Pulls latest code into the Databricks Repo
2. Uploads CSV sources to `cli_catalog.bronze.sources` Volume
3. Upserts the `medallion-platform-controller` job from `databricks/workflows/main_workflow.json`
4. Triggers a pipeline run

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Workspace URL, e.g. `https://dbc-xxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token or Service Principal token |
| `DATABRICKS_REPO_ID` | Numeric ID of the Databricks Repo (Repos UI → kebab menu → Copy ID) |

## Emergency deploy (skip CI)

```bash
# Push to GitHub first, then sync the Repo immediately:
git push && ./scripts/import_notebooks.sh --sync-only

# For mid-debug iteration (uploads local file directly, then push):
./scripts/import_notebooks.sh
git push
```

## Catalog layout

| Schema | Content |
|---|---|
| `controller` | `ingestions` config table, `execution_log` |
| `bronze` | Raw Delta tables + `sources` Volume (CSV files) |
| `silver` | dbt incremental models (cleaned, typed, deduplicated) |
| `gold` | dbt table models (KPIs, aggregations) |

## Active demo sources

| Source | Type | Target table | Active |
|---|---|---|---|
| Open Meteo — São Paulo hourly weather | API | `bronze.weather_raw` | yes |
| IBGE Municipalities | CSV | `bronze.ibge_municipios_raw` | yes |
| PostgreSQL orders (demo) | JDBC/SQL | `bronze.orders_raw` | no — requires DB |

## Monitoring

```sql
-- Last 24h execution status
SELECT ingestion_name, status, rows_written, started_at
FROM cli_catalog.controller.execution_log
WHERE started_at >= NOW() - INTERVAL 24 HOURS
ORDER BY started_at DESC;
```
