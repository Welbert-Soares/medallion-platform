# Databricks notebook source
# Setup inicial — cria schemas e tabelas de controle.
# Idempotente: seguro para re-executar.
# Após rodar este notebook, execute sync_config.py para popular controller.ingestions.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS cli_catalog.controller COMMENT 'Control tables for the metadata-driven ingestion pipeline'")
spark.sql("CREATE SCHEMA IF NOT EXISTS cli_catalog.bronze    COMMENT 'Raw ingested data — no business transformations applied'")
spark.sql("CREATE SCHEMA IF NOT EXISTS cli_catalog.silver    COMMENT 'Cleaned and typed data managed by dbt Core'")
spark.sql("CREATE SCHEMA IF NOT EXISTS cli_catalog.gold      COMMENT 'Aggregated KPIs and analytics-ready models managed by dbt Core'")
print("Schemas ready.")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS cli_catalog.controller.ingestions (
  -- Identity
  ingestion_name        STRING  NOT NULL,
  source_name           STRING  NOT NULL,
  -- Source
  source_type           STRING  NOT NULL,
  source_connection_id  STRING,
  source_schema         STRING,
  -- Target
  target_container      STRING  NOT NULL,
  target_schema         STRING  NOT NULL,
  -- Table metadata
  table_name_target     STRING  NOT NULL,
  table_description     STRING,
  table_type_load       STRING  NOT NULL,
  table_merge_columns   STRING,
  table_active          BOOLEAN NOT NULL,
  -- Params
  table_params_flag_distinct  BOOLEAN,
  table_params_flag_hash_row  BOOLEAN,
  -- API-specific
  table_url_link              STRING,
  table_url_mode              STRING,
  table_url_params            STRING,
  table_url_flatten_method    STRING,
  table_url_data_key          STRING,
  table_url_watermark_column  STRING,
  table_url_last_watermark    TIMESTAMP,
  -- CSV-specific
  table_csv_path              STRING,
  table_csv_delimiter         STRING,
  table_csv_header            BOOLEAN,
  -- SQL-specific
  table_sql_query             STRING,
  table_sql_filter            STRING,
  table_sql_watermark_column  STRING,
  table_sql_last_watermark    TIMESTAMP,
  table_sql_partition_column  STRING,
  -- Control
  sync_at               TIMESTAMP,
  CONSTRAINT pk_ingestion_name PRIMARY KEY (ingestion_name)
)
USING DELTA
COMMENT 'Central catalog of ingestion pipeline configurations. Source of truth: config/controller/*.yml'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS cli_catalog.controller.execution_log (
  log_id         STRING,
  ingestion_name STRING    NOT NULL,
  execution_id   STRING,
  started_at     TIMESTAMP NOT NULL,
  finished_at    TIMESTAMP NOT NULL,
  status         STRING    NOT NULL,
  rows_read      LONG,
  rows_written   LONG,
  error_message  STRING
)
USING DELTA
COMMENT 'Execution log for all ingestion pipeline runs'
""")

print("Tables ready.")

# COMMAND ----------

spark.sql("""
  CREATE VOLUME IF NOT EXISTS cli_catalog.bronze.sources
  COMMENT 'Static source files (CSV, JSON) for ingestion'
""")
print("Volume cli_catalog.bronze.sources ready.")
print("Next step: upload source files then run sync_config.py.")
