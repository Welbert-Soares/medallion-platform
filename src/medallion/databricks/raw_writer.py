"""`RawWriter` adapter: Raw is append-only (ADR 0002)."""

from collections.abc import Mapping

from pyspark.sql import DataFrame, functions as F


class DeltaRawWriter:
    def append(self, table: str, data: DataFrame, stamp: Mapping[str, object]) -> int:
        df = data
        for column, value in stamp.items():
            col = F.lit(value)
            df = df.withColumn(column, col.cast("bigint") if isinstance(value, int) else col)
        rows = df.count()
        # mergeSchema: tracking columns appear on tables loaded before they existed (nulls there).
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)
        return rows
