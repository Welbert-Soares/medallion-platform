"""`run_ingestion`: one Ingestion Run, from Ingestion State to Raw and back (ADR 0002, 0003)."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from medallion.controller.clock import Clock
from medallion.controller.config.model import Ingestion
from medallion.controller.runtime import IngestionCatalog, IngestionRun, IngestionState, RunLog


@dataclass(frozen=True)
class ReadResult:
    data: Any  # a Spark DataFrame; Spark is not abstracted, only kept out of this module
    rows_read: int
    watermark: str | None = None  # how far this read got (e.g. max of the watermark column)


class SourceReader(Protocol):
    def fingerprint(self, ingestion: Ingestion) -> str | None:
        """Identity of the source content (e.g. file hash), or None when the source has none."""
        ...

    def read(self, ingestion: Ingestion, watermark: str | None) -> ReadResult: ...


class RawWriter(Protocol):
    def append(self, table: str, data: Any, stamp: Mapping[str, object]) -> int:
        """Append `data` with the `stamp` columns added; return the rows written."""
        ...


def run_ingestion(
    name: str,
    run_id: int,
    task_run_id: int,
    *,
    catalog: IngestionCatalog,
    readers: Mapping[str, SourceReader],
    writer: RawWriter,
    run_log: RunLog,
    clock: Clock,
) -> IngestionRun:
    """Read the source, append it to Raw, then advance the Ingestion State and log the run.

    The state only advances after Raw was written. On any error the failure is logged, the
    state is left as it was, and the error is re-raised so the job task fails.
    """
    started_at = clock.now()
    before: str | None = None
    rows_read = rows_written = 0

    def log(status, after, error=None) -> IngestionRun:
        run = IngestionRun(run_id, task_run_id, name, status, rows_read, rows_written, before,
                           after, error, started_at, clock.now())
        run_log.record(run)
        return run

    try:
        ingestion = catalog.get(name)
        state = catalog.get_state(name)
        before = state.watermark if state else None
        reader = readers.get(ingestion.source.type)
        if reader is None:
            raise ValueError(f"no reader for source type '{ingestion.source.type}'")

        fingerprint = reader.fingerprint(ingestion)
        if fingerprint is not None and fingerprint == before:
            return log("skipped", before)

        result = reader.read(ingestion, before)
        rows_read = result.rows_read
        if rows_read:
            stamp = {"_run_id": run_id, "_ingested_at": started_at}
            rows_written = writer.append(catalog.raw_table_of(ingestion), result.data, stamp)

        after = fingerprint if fingerprint is not None else (result.watermark or before)
        catalog.save_state(IngestionState(name, after, run_id, clock.now()))
    except Exception as e:
        log("failed", before, f"{type(e).__name__}: {e}")
        raise
    return log("success", after)
