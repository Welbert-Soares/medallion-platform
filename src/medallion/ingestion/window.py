"""Incremental date window of an API Ingestion."""

from dataclasses import dataclass
from datetime import date, timedelta

from medallion.controller.config.model import ApiWindow


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date


def resolve_window(window: ApiWindow, watermark: str | None, today: date) -> DateWindow | None:
    """Day after the Watermark (or the declared start date) up to today minus the lag.

    None when the window is empty, i.e. the Ingestion is already up to date.
    """
    start = date.fromisoformat(watermark) + timedelta(days=1) if watermark else window.start_date
    end = today - timedelta(days=window.lag_days)
    return DateWindow(start, end) if start <= end else None
