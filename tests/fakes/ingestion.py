"""Fakes for `run_ingestion`: data is a list of dicts standing in for a DataFrame."""

from medallion.controller.runtime import IngestionRun, IngestionState
from medallion.ingestion.run import ReadResult


class FakeIngestionCatalog:
    def __init__(self, ingestions, states=(), raw_catalog="medallion_raw"):
        self.ingestions = {i.name: i for i in ingestions}
        self.states = {s.ingestion_name: s for s in states}
        self.raw_catalog = raw_catalog

    def active(self):
        return [i for i in self.ingestions.values() if i.active]

    def get(self, name):
        return self.ingestions[name]

    def raw_table_of(self, ingestion):
        return f"{self.raw_catalog}.{ingestion.source_system}.{ingestion.raw_table}"

    def get_state(self, name) -> IngestionState | None:
        return self.states.get(name)

    def save_state(self, state: IngestionState) -> None:
        self.states[state.ingestion_name] = state


class FakeRunLog:
    def __init__(self):
        self.runs: list[IngestionRun] = []

    def record(self, run: IngestionRun) -> None:
        self.runs.append(run)


class FakeReader:
    def __init__(self, rows, fingerprint=None, watermark=None, error=None):
        self.rows = rows
        self._fingerprint = fingerprint
        self.watermark = watermark
        self.error = error
        self.reads: list[str | None] = []  # watermark passed to each read

    def fingerprint(self, ingestion):
        return self._fingerprint

    def read(self, ingestion, watermark):
        self.reads.append(watermark)
        if self.error:
            raise self.error
        return ReadResult(list(self.rows), len(self.rows), self.watermark)


class FakeRawWriter:
    def __init__(self, error=None):
        self.tables: dict[str, list[dict]] = {}
        self.error = error

    def append(self, table, data, stamp) -> int:
        if self.error:
            raise self.error
        rows = [{**row, **stamp} for row in data]
        self.tables.setdefault(table, []).extend(rows)
        return len(rows)
