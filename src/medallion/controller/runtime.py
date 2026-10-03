"""What the Controller records at run time: Ingestion State and the execution log (ADR 0003).

These ports are consumed by the Medallion (`medallion.ingestion`); the Controller never runs
an Ingestion itself.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from medallion.controller.config.model import Ingestion


@dataclass(frozen=True)
class IngestionState:
    """How far an Ingestion has landed in Raw. Written only by the runtime, never by config sync."""

    ingestion_name: str
    watermark: str | None  # incremental column value, or the source file fingerprint
    last_run_id: int | None
    updated_at: datetime


Status = Literal["success", "skipped", "failed"]


@dataclass(frozen=True)
class IngestionRun:
    """One row of the execution log: one Ingestion executed inside a Run."""

    run_id: int
    task_run_id: int
    ingestion_name: str
    status: Status
    rows_read: int
    rows_written: int
    watermark_before: str | None
    watermark_after: str | None
    error: str | None
    started_at: datetime
    finished_at: datetime


INGESTION_STATE_COLUMNS: dict[str, str] = {
    "ingestion_name": "STRING NOT NULL",
    "watermark": "STRING",
    "last_run_id": "BIGINT",
    "updated_at": "TIMESTAMP NOT NULL",
}

EXECUTION_LOG_COLUMNS: dict[str, str] = {
    "run_id": "BIGINT NOT NULL",
    "task_run_id": "BIGINT NOT NULL",
    "ingestion_name": "STRING NOT NULL",
    "status": "STRING NOT NULL",
    "rows_read": "BIGINT NOT NULL",
    "rows_written": "BIGINT NOT NULL",
    "watermark_before": "STRING",
    "watermark_after": "STRING",
    "error": "STRING",
    "started_at": "TIMESTAMP NOT NULL",
    "finished_at": "TIMESTAMP NOT NULL",
}


class IngestionCatalog(Protocol):
    def active(self) -> list[Ingestion]: ...

    def get(self, name: str) -> Ingestion:
        """Raises KeyError for an undeclared Ingestion."""
        ...

    def raw_table_of(self, ingestion: Ingestion) -> str: ...

    def get_state(self, name: str) -> IngestionState | None: ...

    def save_state(self, state: IngestionState) -> None: ...


class RunLog(Protocol):
    def record(self, run: IngestionRun) -> None: ...
