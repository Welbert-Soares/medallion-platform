# Platform metrics are a dbt model; the heartbeat runs outside Databricks

The platform measures itself with its own tools: `medallion_gold.platform.run_metrics` is a dbt model over the Controller's `execution_log` (one row per Run), built by a `dbt_platform` task after every ingest, failed ones included. A GitHub Actions workflow (`site.yml`) runs daily at 06:30, after the 06:00 Run, and publishes it as `metrics.json` on GitHub Pages next to the dbt docs, by querying the table through the SQL Statement API.

That workflow is also the heartbeat: it fails when no Run finished in the last 26 hours, and GitHub emails the failure. We put it outside Databricks because every check inside the job (tests, freshness, the `ingestions_succeeded` leaf) only runs when the job runs; between 2026-10-08 and 2026-11-02 the scheduled job silently did not trigger, and nothing inside the workspace could notice. Querying from outside also needs no outbound network access from the workspace, which we could not rely on in the Free Edition.

## Consequences

- `gold.platform` is a Layer schema declared in `catalog_structure.yml`, not a Source System; its dbt task is written by hand in `databricks.yml`, outside the generated block.
- The heartbeat only fails on the scheduled run, never on a deploy: a deploy after a quiet period is not an incident.
- Until the first Run after a fresh deploy creates `run_metrics`, `metrics.json` is published with no Runs.
