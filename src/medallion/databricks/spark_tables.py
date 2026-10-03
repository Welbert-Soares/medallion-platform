"""Small Delta helpers shared by the Controller adapters."""

from pyspark.sql import DataFrame, SparkSession


def ddl(columns: dict[str, str]) -> str:
    return ", ".join(f"`{name}` {type_}" for name, type_ in columns.items())


def rows_frame(spark: SparkSession, columns: dict[str, str], rows: list[dict]) -> DataFrame:
    return spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], ddl(columns))


def create_table_if_not_exists(
    spark: SparkSession, table: str, columns: dict[str, str], comment: str
) -> None:
    # Row-level concurrency, so parallel Ingestion Runs can update different rows at once.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({ddl(columns)}) USING DELTA "
        f"COMMENT '{comment}' TBLPROPERTIES ("
        "'delta.enableDeletionVectors' = 'true', 'delta.enableRowTracking' = 'true')"
    )
