# Controller and Medallion are separate modules with a one-way dependency

The `medallion` package has two top-level modules: `medallion.controller` (config, Catalog Structure, Ingestion State, execution log) and `medallion.ingestion` (reading sources, writing Raw). Ingestion depends on the Controller only through its ports; the Controller never imports Ingestion, and only `medallion.databricks` may import pyspark, databricks-sdk or dbutils. Both rules are enforced by ruff in CI. We kept one wheel rather than two packages because both are always deployed together.
