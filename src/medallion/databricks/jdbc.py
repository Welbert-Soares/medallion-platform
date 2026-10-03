"""`JdbcClient` over Spark's JDBC source, and `SecretProvider` over dbutils secrets."""

from datetime import date, datetime

from pyspark.sql import SparkSession, functions as F

from medallion.databricks.sources import normalize_column_names
from medallion.ingestion.jdbc import JdbcRequest
from medallion.ingestion.run import ReadResult

_NUM_PARTITIONS = 8


class SparkJdbcClient:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, request: JdbcRequest) -> ReadResult:
        reader = self._reader(request).option("dbtable", f"({request.query}) src")
        if request.partition_column:
            lower, upper = self._bounds(request)
            if lower is not None:
                reader = (reader
                          .option("partitionColumn", request.partition_column)
                          .option("lowerBound", str(lower))
                          .option("upperBound", str(upper))
                          .option("numPartitions", _NUM_PARTITIONS))
        df = reader.load()

        aggregates = [F.count(F.lit(1)).alias("rows")]
        if request.watermark_column:
            aggregates.append(F.max(request.watermark_column).alias("watermark"))
        stats = df.agg(*aggregates).first()
        watermark = _as_watermark(stats["watermark"]) if request.watermark_column else None
        return ReadResult(normalize_column_names(df), stats["rows"], watermark)

    def _reader(self, request: JdbcRequest):
        return (self.spark.read.format("jdbc")
                .option("url", request.url)
                .option("driver", request.driver)
                .option("user", request.user)
                .option("password", request.password))

    def _bounds(self, request: JdbcRequest):
        col = request.partition_column
        bounds = (self._reader(request)
                  .option("query", f"SELECT MIN({col}) AS lo, MAX({col}) AS hi "
                                   f"FROM ({request.query}) b")
                  .load().first())
        return bounds["lo"], bounds["hi"]


def _as_watermark(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


class DbutilsSecretProvider:
    def get(self, scope: str, key: str) -> str:
        from databricks.sdk.runtime import dbutils  # only resolvable inside a job run

        return dbutils.secrets.get(scope=scope, key=key)
