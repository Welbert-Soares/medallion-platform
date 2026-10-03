"""`SourceReader` adapters."""

import hashlib
import re

from pyspark.sql import DataFrame, SparkSession

from medallion.controller.config.model import CsvSource, Ingestion
from medallion.ingestion.run import ReadResult


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Lowercase column names and replace non-alphanumeric characters with underscores."""
    for col in df.columns:
        clean = re.sub(r"_+", "_", re.sub(r"[^a-z0-9]", "_", col.lower().strip())).strip("_")
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df


def file_fingerprint(path: str) -> str:
    """Content hash, so re-uploading an identical file does not reload it."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


class CsvReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def fingerprint(self, ingestion: Ingestion) -> str | None:
        return file_fingerprint(self._source(ingestion).path)

    def read(self, ingestion: Ingestion, watermark: str | None) -> ReadResult:
        source = self._source(ingestion)
        df = (self.spark.read
              .option("delimiter", source.delimiter)
              .option("header", str(source.header).lower())
              .option("inferSchema", "true")
              .csv(source.path))
        df = normalize_column_names(df)
        return ReadResult(df, df.count())

    @staticmethod
    def _source(ingestion: Ingestion) -> CsvSource:
        if not isinstance(ingestion.source, CsvSource):
            raise TypeError(f"{ingestion.name} is not a csv ingestion")
        return ingestion.source
