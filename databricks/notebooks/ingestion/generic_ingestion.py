# Databricks notebook source
# Motor de ingestão genérico. Recebe `ingestion_name` como parâmetro de job.
# A configuração é lida de cli_catalog.controller.ingestions (populada pelo sync_config.py).

# COMMAND ----------

dbutils.widgets.text("ingestion_name", "")
ingestion_name = dbutils.widgets.get("ingestion_name")

if not ingestion_name:
    raise ValueError("Widget 'ingestion_name' é obrigatório.")

# COMMAND ----------

import sys
import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType,
)

REPO_ROOT = "/Workspace/Repos/welbertsoares6@gmail.com/medallion-platform"
sys.path.insert(0, REPO_ROOT)
from config.utils.delta_utils import normalize_column_names, write_to_delta

LOG_SCHEMA = StructType([
    StructField("log_id",         StringType(),    True),
    StructField("ingestion_name", StringType(),    False),
    StructField("execution_id",   StringType(),    True),
    StructField("started_at",     TimestampType(), False),
    StructField("finished_at",    TimestampType(), False),
    StructField("status",         StringType(),    False),
    StructField("rows_read",      LongType(),      True),
    StructField("rows_written",   LongType(),      True),
    StructField("error_message",  StringType(),    True),
])

# COMMAND ----------
# 1. Carregar configuração da controller

config_row = spark.sql(f"""
    SELECT * FROM cli_catalog.controller.ingestions
    WHERE ingestion_name = '{ingestion_name}' AND table_active = true
""").first()

if not config_row:
    raise ValueError(f"Ingestão '{ingestion_name}' não encontrada ou inativa.")

source_type   = config_row["source_type"]
target_table  = f"cli_catalog.{config_row['target_container']}.{config_row['table_name_target']}"
load_mode     = config_row["table_type_load"]
merge_key     = config_row["table_merge_columns"]
flag_distinct = bool(config_row["table_params_flag_distinct"])

# Config específica por tipo de fonte
if source_type == "api":
    api_link     = config_row["table_url_link"]
    api_params   = json.loads(config_row["table_url_params"] or "{}")
    api_flatten  = config_row["table_url_flatten_method"] or "default"
    api_data_key = config_row["table_url_data_key"]
    wm_col       = config_row["table_url_watermark_column"]
    last_wm      = config_row["table_url_last_watermark"]
    wm_col_name  = "table_url_last_watermark"

elif source_type == "csv":
    csv_path  = config_row["table_csv_path"]
    csv_delim = config_row["table_csv_delimiter"] or ","
    csv_hdr   = config_row["table_csv_header"]
    wm_col    = None
    last_wm   = None
    wm_col_name = None

elif source_type == "sql":
    sql_query   = config_row["table_sql_query"]
    sql_filter  = config_row["table_sql_filter"]
    conn_id     = config_row["source_connection_id"]
    wm_col      = config_row["table_sql_watermark_column"]
    last_wm     = config_row["table_sql_last_watermark"]
    wm_col_name = "table_sql_last_watermark"

print(f"[{ingestion_name}] source_type={source_type} | target={target_table} | mode={load_mode}")

# COMMAND ----------
# 2. Leitores por tipo de fonte

def read_api(link, params, flatten_method, data_key, wm_col, last_wm):
    import requests
    if wm_col and last_wm:
        params[wm_col] = str(last_wm)
    resp = requests.get(link, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if flatten_method == "zip_arrays":
        nested = data[data_key]
        keys   = list(nested.keys())
        rows   = [dict(zip(keys, vals)) for vals in zip(*[nested[k] for k in keys])]
    elif isinstance(data, list):
        rows = data
    else:
        rows = [data]

    return spark.createDataFrame(rows)


def read_csv(path, delimiter, header):
    return (spark.read
            .option("delimiter", delimiter)
            .option("header",    str(header))
            .option("inferSchema", "true")
            .csv(path))


def read_sql(query, sql_filter, conn_id, wm_col, last_wm):
    # Substituição do placeholder de watermark
    if wm_col and last_wm and "?" in query:
        query = query.replace("?", f"'{last_wm}'")
    if sql_filter:
        query = f"{query} {sql_filter}"
    # Conexão via tabela controller.connections — disponível na Fase 3
    raise NotImplementedError(
        f"Fonte SQL requer configuração de connections (Fase 3). conn_id={conn_id}"
    )

# COMMAND ----------
# 3. Ler dados, escrever no Bronze, registrar log

start_time = datetime.now()
rows_read  = 0

try:
    if source_type == "api":
        df = read_api(api_link, api_params, api_flatten, api_data_key, wm_col, last_wm)
    elif source_type == "csv":
        df = read_csv(csv_path, csv_delim, csv_hdr)
    elif source_type == "sql":
        df = read_sql(sql_query, sql_filter, conn_id, wm_col, last_wm)
    else:
        raise ValueError(f"source_type desconhecido: {source_type}")

    if flag_distinct:
        df = df.distinct()

    df = normalize_column_names(df)
    df = (df
          .withColumn("_ingested_at",    F.current_timestamp())
          .withColumn("_ingestion_name", F.lit(ingestion_name)))

    # ── Escrita no Bronze ──────────────────────────────────────────────────
    rows_read = write_to_delta(
        spark,
        df,
        target_table,
        type_load     = load_mode,
        merge_columns = merge_key,
    )
    print(f"[{ingestion_name}] linhas escritas: {rows_read}")

    # ── Atualização de watermark ───────────────────────────────────────────
    if wm_col and wm_col_name:
        new_wm = df.agg(F.max(wm_col)).first()[0]
        spark.sql(f"""
            UPDATE cli_catalog.controller.ingestions
            SET {wm_col_name} = '{new_wm}', sync_at = current_timestamp()
            WHERE ingestion_name = '{ingestion_name}'
        """)
        print(f"[{ingestion_name}] watermark → {new_wm}")

    spark.createDataFrame([{
        "log_id":         str(datetime.now().timestamp()),
        "ingestion_name": ingestion_name,
        "execution_id":   None,
        "started_at":     start_time,
        "finished_at":    datetime.now(),
        "status":         "success",
        "rows_read":      rows_read,
        "rows_written":   rows_read,
        "error_message":  None,
    }], schema=LOG_SCHEMA).write.format("delta").mode("append").saveAsTable("cli_catalog.controller.execution_log")
    print(f"[{ingestion_name}] SUCESSO — {rows_read} linhas → {target_table}")

except Exception as e:
    spark.createDataFrame([{
        "log_id":         str(datetime.now().timestamp()),
        "ingestion_name": ingestion_name,
        "execution_id":   None,
        "started_at":     start_time,
        "finished_at":    datetime.now(),
        "status":         "failed",
        "rows_read":      rows_read,
        "rows_written":   0,
        "error_message":  str(e)[:2000],
    }], schema=LOG_SCHEMA).write.format("delta").mode("append").saveAsTable("cli_catalog.controller.execution_log")
    raise
