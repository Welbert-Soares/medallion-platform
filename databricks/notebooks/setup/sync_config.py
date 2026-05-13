# Databricks notebook source
# Lê todos os YAMLs de config/controller/*.yml e faz upsert em controller.ingestions.
# Executado automaticamente como primeira task do job (antes da ingestão).

# COMMAND ----------

# MAGIC %pip install cerberus pyyaml --quiet

# COMMAND ----------

import os
import sys
import yaml
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

REPO_ROOT    = "/Workspace/Repos/welbertsoares6@gmail.com/medallion-platform"
CONFIG_BASE  = f"{REPO_ROOT}/config"
SCHEMA_PATH  = f"{CONFIG_BASE}/controller/schemas/ingestions.yml"

# Adiciona a raiz do repo ao path para importar config.utils.*
sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

from config.utils.extract_controller import ExtractController

# Coletar todos os YAMLs de config/controller/ (excluindo connections.yml e a pasta schemas/)
yaml_files = [
    os.path.join(f"{CONFIG_BASE}/controller", f)
    for f in os.listdir(f"{CONFIG_BASE}/controller")
    if f.endswith((".yml", ".yaml"))
    and not f.startswith("connections")
    and f != "schemas"
    and os.path.isfile(os.path.join(f"{CONFIG_BASE}/controller", f))
]

print(f"Arquivos encontrados ({len(yaml_files)}): {[os.path.basename(f) for f in yaml_files]}")

# COMMAND ----------

lst_ingestions = []

for file_path in yaml_files:
    with open(file_path, "r", encoding="utf-8") as f:
        content = yaml.safe_load(f)

    if "ingestions" not in content:
        print(f"  [SKIP] {os.path.basename(file_path)} — sem chave 'ingestions'")
        continue

    ec     = ExtractController(SCHEMA_PATH, content)
    rows   = ec.get_ingestions()
    lst_ingestions.extend(rows)
    print(f"  [OK]   {os.path.basename(file_path)}: {len(rows)} ingestão(ões)")

print(f"\nTotal: {len(lst_ingestions)} ingestão(ões) para sincronizar")

# COMMAND ----------

# Garantir que todas as colunas nullable existam no dict (preenchidas com None)
NULLABLE_COLS = [
    "source_connection_id", "source_schema",
    "table_merge_columns", "table_description",
    "table_params_flag_distinct", "table_params_flag_hash_row",
    "table_url_link", "table_url_mode", "table_url_params",
    "table_url_flatten_method", "table_url_data_key",
    "table_url_watermark_column", "table_url_last_watermark",
    "table_csv_path", "table_csv_delimiter", "table_csv_header",
    "table_sql_query", "table_sql_filter",
    "table_sql_watermark_column", "table_sql_last_watermark",
    "table_sql_partition_column",
]

sync_ts = datetime.now()
for row in lst_ingestions:
    row["sync_at"] = sync_ts
    for col in NULLABLE_COLS:
        row.setdefault(col, None)

# COMMAND ----------

df_new = spark.createDataFrame(lst_ingestions)

# Cast de tipos para bater com o schema Delta
df_new = (df_new
    .withColumn("table_active",               F.col("table_active").cast("boolean"))
    .withColumn("table_params_flag_distinct",  F.col("table_params_flag_distinct").cast("boolean"))
    .withColumn("table_params_flag_hash_row",  F.col("table_params_flag_hash_row").cast("boolean"))
    .withColumn("table_csv_header",            F.col("table_csv_header").cast("boolean"))
    .withColumn("table_url_last_watermark",    F.col("table_url_last_watermark").cast("timestamp"))
    .withColumn("table_sql_last_watermark",    F.col("table_sql_last_watermark").cast("timestamp"))
    .withColumn("sync_at",                     F.col("sync_at").cast("timestamp"))
    .withColumn("table_name_target",           F.lower(F.col("table_name_target")))
)

display(df_new.select(
    "ingestion_name", "source_name", "source_type",
    "table_name_target", "table_active", "table_type_load"
))

# COMMAND ----------

# Upsert (MERGE) em controller.ingestions — chave: ingestion_name
delta_tbl = DeltaTable.forName(spark, "cli_catalog.controller.ingestions")

(delta_tbl.alias("t")
    .merge(df_new.alias("s"), "t.ingestion_name = s.ingestion_name")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

print(f"Sync concluído: {len(lst_ingestions)} ingestão(ões) upsertadas em cli_catalog.controller.ingestions")
display(spark.sql("""
    SELECT ingestion_name, source_name, source_type, table_name_target, table_active, sync_at
    FROM cli_catalog.controller.ingestions
    ORDER BY source_name, ingestion_name
"""))
