import re
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase column names and replace non-alphanumeric chars with underscore."""
    result = df
    for col in df.columns:
        clean = re.sub(r"[^a-z0-9]", "_", col.lower().strip())
        clean = re.sub(r"_+", "_", clean).strip("_")
        if clean != col:
            result = result.withColumnRenamed(col, clean)
    return result


def write_to_delta(
    spark,
    df: DataFrame,
    target_table: str,
    type_load: str,
    merge_columns: str | None = None,
    replace_where: str | None = None,
) -> int:
    """
    Write df to a Delta table.  Returns row count written.

    type_load values:
      append        – append rows (default for time-series / event sources)
      overwrite     – full table replace (schema evolution via overwriteSchema)
      merge         – upsert; merge_columns = comma-separated key column(s)
      replace_where – atomic partition overwrite; replace_where = SQL predicate
    """
    rows = df.count()

    writer = df.write.format("delta").option("mergeSchema", "true")

    if type_load == "append":
        writer.mode("append").saveAsTable(target_table)

    elif type_load == "overwrite":
        (writer
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(target_table))

    elif type_load == "replace_where":
        if not replace_where:
            raise ValueError(
                "replace_where predicate obrigatório para type_load='replace_where'."
            )
        (writer
         .mode("overwrite")
         .option("replaceWhere", replace_where)
         .saveAsTable(target_table))

    elif type_load == "merge":
        if not merge_columns:
            raise ValueError(
                "merge_columns obrigatório para type_load='merge'."
            )
        keys = [k.strip() for k in merge_columns.split(",")]
        condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)

        if DeltaTable.isDeltaTable(spark, target_table):
            dt = DeltaTable.forName(spark, target_table)
            (dt.alias("t")
             .merge(df.alias("s"), condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        else:
            # Primeira carga: cria a tabela via overwrite
            (writer
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable(target_table))

    else:
        raise ValueError(f"type_load desconhecido: '{type_load}'")

    return rows
