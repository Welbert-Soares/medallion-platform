# Medallion Platform

Metadata-driven data platform on Databricks. Source Systems declared in YAML are ingested as-is into
an append-only **raw** Layer and refined by dbt through **bronze → silver → gold**, one Unity Catalog
catalog per Layer (`medallion_raw`, `medallion_bronze`, …).

The platform is split in two (ADR 0005):

- **Controller** declares, configures and records: config, Catalog Structure (catalogs, schemas,
  volumes, grants), Ingestion State and the execution log, in `medallion_controller`.
- **Medallion** moves and refines data: Ingestions into Raw, then the dbt projects.

Vocabulary lives in [`CONTEXT.md`](CONTEXT.md); architectural decisions in [`docs/adr/`](docs/adr/).

## How it runs

One Databricks job, defined in [`databricks.yml`](databricks.yml) and deployed as an Asset Bundle with
the `medallion` wheel, runs daily at 06:00 America/Sao_Paulo:

```
sync_config ─► ingest (for-each over active Ingestions) ─┬─► dbt bronze_ibge ─► silver_ibge
                                                          ├─► dbt bronze_weather ─► silver_weather ─► gold_weather
                                                          └─► ingestions_succeeded
```

Merging to `main` applies the Catalog Structure, ensures secret scopes, uploads source files and
deploys the bundle. It never triggers a Run.

## Adding a source

1. Declare the Source System in [`config/platform/catalog_structure.yml`](config/platform/catalog_structure.yml)
   (it becomes a schema in every Layer).
2. Add its Ingestions to `config/platform/ingestions/<source_system>.yml`; each lands in
   `medallion_raw.<source_system>.<raw_table>`.
3. For databases, declare a Connection and put its password by hand: see
   [`docs/connections.md`](docs/connections.md).
4. Add dbt projects `dbt/bronze_<system>`, `dbt/silver_<system>`, … and their tasks in `databricks.yml`.

## Development

```bash
make install    # dev environment
make lint       # ruff, including import boundaries
make test       # pytest, no Spark cluster needed
make validate   # config + bundle definition
make plan       # Catalog Structure diff against the workspace (dry-run)
make deploy     # apply + bundle deploy from your machine (CI does this on merge)
```

`validate`, `plan` and `deploy` read `DATABRICKS_HOST` / `DATABRICKS_TOKEN` from `.env`
(see [`.env.example`](.env.example)).
