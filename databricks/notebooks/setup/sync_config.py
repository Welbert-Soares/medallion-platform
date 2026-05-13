# Databricks notebook source
# Lê todos os YAMLs de config/controller/*.yml e faz upsert em controller.ingestions.
# Executado automaticamente como primeira task do job (antes da ingestão).

# COMMAND ----------

# MAGIC %pip install cerberus pyyaml --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, TimestampType,
)
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

INGESTIONS_SCHEMA = StructType([
    StructField("ingestion_name",              StringType(),    False),
    StructField("source_name",                 StringType(),    False),
    StructField("source_type",                 StringType(),    False),
    StructField("source_connection_id",        StringType(),    True),
    StructField("source_schema",               StringType(),    True),
    StructField("target_container",            StringType(),    False),
    StructField("target_schema",               StringType(),    False),
    StructField("table_name_target",           StringType(),    False),
    StructField("table_description",           StringType(),    True),
    StructField("table_type_load",             StringType(),    False),
    StructField("table_merge_columns",         StringType(),    True),
    StructField("table_active",                BooleanType(),   False),
    StructField("table_params_flag_distinct",  BooleanType(),   True),
    StructField("table_params_flag_hash_row",  BooleanType(),   True),
    StructField("table_url_link",              StringType(),    True),
    StructField("table_url_mode",              StringType(),    True),
    StructField("table_url_params",            StringType(),    True),
    StructField("table_url_flatten_method",    StringType(),    True),
    StructField("table_url_data_key",          StringType(),    True),
    StructField("table_url_watermark_column",  StringType(),    True),
    StructField("table_url_last_watermark",    TimestampType(), True),
    StructField("table_csv_path",              StringType(),    True),
    StructField("table_csv_delimiter",         StringType(),    True),
    StructField("table_csv_header",            BooleanType(),   True),
    StructField("table_sql_query",             StringType(),    True),
    StructField("table_sql_filter",            StringType(),    True),
    StructField("table_sql_watermark_column",  StringType(),    True),
    StructField("table_sql_last_watermark",    TimestampType(), True),
    StructField("table_sql_partition_column",  StringType(),    True),
    StructField("sync_at",                     TimestampType(), True),
])

df_new = spark.createDataFrame(lst_ingestions, schema=INGESTIONS_SCHEMA)
df_new = df_new.withColumn("table_name_target", F.lower(F.col("table_name_target")))

display(df_new.select(
    "ingestion_name", "source_name", "source_type",
    "table_name_target", "table_active", "table_type_load"
))

# COMMAND ----------

# Upsert (MERGE) em controller.ingestions — chave: ingestion_name
delta_tbl = DeltaTable.forName(spark, "medallion_controller.delta_controller.ingestions")

(delta_tbl.alias("t")
    .merge(df_new.alias("s"), "t.ingestion_name = s.ingestion_name")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

print(f"Sync concluído: {len(lst_ingestions)} ingestão(ões) upsertadas em medallion_controller.delta_controller.ingestions")
display(spark.sql("""
    SELECT ingestion_name, source_name, source_type, table_name_target, table_active, sync_at
    FROM medallion_controller.delta_controller.ingestions
    ORDER BY source_name, ingestion_name
"""))
