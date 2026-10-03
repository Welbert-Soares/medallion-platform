"""Seam 2: `run_ingestion` with fakes for every port."""

from datetime import date, datetime, timezone

import pytest
from fakes.clock import FixedClock
from fakes.ingestion import FakeIngestionCatalog, FakeRawWriter, FakeReader, FakeRunLog

from medallion.controller.config import Ingestion
from medallion.controller.runtime import IngestionState
from medallion.ingestion.run import run_ingestion
from medallion.ingestion.window import DateWindow

NOW = datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc)
RUN_ID, TASK_RUN_ID = 823902144008677, 911111111111
RAW = "medallion_raw.ibge.municipios"
ROWS = [{"codigo_ibge": 1, "nome": "Alta Floresta"}, {"codigo_ibge": 2, "nome": "Ariquemes"}]

CSV = Ingestion.model_validate({
    "name": "ibge_municipios", "source_system": "ibge", "raw_table": "municipios",
    "active": True, "source": {"type": "csv", "path": "/Volumes/x/m.csv"}})
SQL = Ingestion.model_validate({
    "name": "orders", "source_system": "orders", "raw_table": "orders", "active": True,
    "source": {"type": "sql", "connection_id": "pg", "query": "SELECT 1",
               "watermark_column": "updated_at"}})


def previous_state(name="ibge_municipios", watermark="sha256:old"):
    return IngestionState(name, watermark, 1, datetime(2026, 10, 2, tzinfo=timezone.utc))


class Harness:
    def __init__(self, ingestion=CSV, states=(), reader=None, writer=None):
        self.catalog = FakeIngestionCatalog([ingestion], states)
        self.reader = reader or FakeReader(ROWS, fingerprint="sha256:new")
        self.writer = writer or FakeRawWriter()
        self.log = FakeRunLog()
        self.ingestion = ingestion

    def run(self):
        return run_ingestion(
            self.ingestion.name, RUN_ID, TASK_RUN_ID, catalog=self.catalog,
            readers={self.ingestion.source.type: self.reader}, writer=self.writer,
            run_log=self.log, clock=FixedClock(NOW))

    @property
    def state(self):
        return self.catalog.states.get(self.ingestion.name)


# ── success ──────────────────────────────────────────────────────────────────


def test_first_run_appends_to_raw_and_records_state_and_log():
    h = Harness()

    run = h.run()

    assert len(h.writer.tables[RAW]) == 2
    assert h.state == IngestionState("ibge_municipios", "sha256:new", RUN_ID, NOW)
    assert h.log.runs == [run]
    assert (run.status, run.run_id, run.task_run_id, run.rows_read, run.rows_written,
            run.watermark_before, run.watermark_after, run.error) == (
        "success", RUN_ID, TASK_RUN_ID, 2, 2, None, "sha256:new", None)
    assert (run.started_at, run.finished_at) == (NOW, NOW)


def test_rows_are_stamped_with_run_id_and_ingestion_time():
    h = Harness()

    h.run()

    assert h.writer.tables[RAW][0] == {
        "codigo_ibge": 1, "nome": "Alta Floresta", "_run_id": RUN_ID, "_ingested_at": NOW}


def test_raw_is_appended_never_replaced():
    h = Harness(states=[previous_state()])
    h.writer.tables[RAW] = [{"codigo_ibge": 1, "_run_id": None, "_ingested_at": None}]

    h.run()

    assert len(h.writer.tables[RAW]) == 3


def test_changed_file_is_loaded_and_fingerprint_advances():
    h = Harness(states=[previous_state(watermark="sha256:old")])

    run = h.run()

    assert (run.status, run.watermark_before, run.watermark_after) == (
        "success", "sha256:old", "sha256:new")
    assert h.state.watermark == "sha256:new"


def test_watermark_from_the_read_advances_when_the_source_has_no_fingerprint():
    reader = FakeReader(ROWS, watermark="2026-10-02T23:00:00")
    h = Harness(SQL, [previous_state("orders", "2026-10-01T00:00:00")], reader=reader)

    run = h.run()

    assert reader.reads == [("2026-10-01T00:00:00", None)]
    assert run.watermark_after == h.state.watermark == "2026-10-02T23:00:00"


def test_empty_read_writes_nothing_and_keeps_the_watermark():
    reader = FakeReader([], watermark=None)
    h = Harness(SQL, [previous_state("orders", "2026-10-01")], reader=reader)

    run = h.run()

    assert h.writer.tables == {}
    assert (run.status, run.rows_read, run.rows_written, run.watermark_after) == (
        "success", 0, 0, "2026-10-01")


# ── skip by fingerprint ──────────────────────────────────────────────────────


def test_unchanged_file_is_skipped_without_reading():
    h = Harness(states=[previous_state(watermark="sha256:new")])
    before = h.state

    run = h.run()

    assert h.reader.reads == []
    assert h.writer.tables == {}
    assert h.state == before
    assert (run.status, run.rows_read, run.rows_written, run.watermark_after) == (
        "skipped", 0, 0, "sha256:new")
    assert h.log.runs == [run]


# ── failure ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("failing", ["reader", "writer"])
def test_failure_is_logged_keeps_state_and_raises(failing):
    boom = RuntimeError("source unavailable")
    h = Harness(
        states=[previous_state()],
        reader=FakeReader(ROWS, fingerprint="sha256:new", error=boom if failing == "reader" else None),
        writer=FakeRawWriter(error=boom if failing == "writer" else None),
    )

    with pytest.raises(RuntimeError, match="source unavailable"):
        h.run()

    assert h.state == previous_state()
    (run,) = h.log.runs
    assert (run.status, run.rows_written, run.watermark_before, run.watermark_after, run.error) == (
        "failed", 0, "sha256:old", "sha256:old", "RuntimeError: source unavailable")


def test_rows_read_are_logged_when_the_write_fails():
    h = Harness(writer=FakeRawWriter(error=OSError("disk")))

    with pytest.raises(OSError):
        h.run()

    assert (h.log.runs[0].rows_read, h.log.runs[0].rows_written) == (2, 0)


def test_undeclared_ingestion_is_logged_as_failure():
    h = Harness()

    with pytest.raises(KeyError):
        run_ingestion("nope", RUN_ID, TASK_RUN_ID, catalog=h.catalog, readers={},
                      writer=h.writer, run_log=h.log, clock=FixedClock(date(2026, 10, 3)))

    assert [(r.ingestion_name, r.status) for r in h.log.runs] == [("nope", "failed")]


def test_source_type_without_reader_fails():
    h = Harness(SQL)

    with pytest.raises(ValueError, match="no reader for source type 'sql'"):
        run_ingestion("orders", RUN_ID, TASK_RUN_ID, catalog=h.catalog, readers={"csv": h.reader},
                      writer=h.writer, run_log=h.log, clock=FixedClock(NOW))

    assert h.log.runs[0].status == "failed"


# ── API window ───────────────────────────────────────────────────────────────

WEATHER = Ingestion.model_validate({
    "name": "weather_sao_paulo", "source_system": "weather", "raw_table": "sao_paulo",
    "active": True, "source": {
        "type": "api", "url": "https://archive.example/v1", "data_key": "hourly",
        "window": {"start_param": "start_date", "end_param": "end_date",
                   "start_date": "2026-01-01", "lag_days": 5}}})
# NOW is 2026-10-03, so with a 5-day lag every window ends on 2026-09-28.


def api_harness(watermark=None, rows=ROWS):
    states = [previous_state("weather_sao_paulo", watermark)] if watermark else []
    return Harness(WEATHER, states, reader=FakeReader(rows))


def test_without_watermark_the_window_starts_at_the_declared_start_date():
    h = api_harness()

    run = h.run()

    assert h.reader.reads == [(None, DateWindow(date(2026, 1, 1), date(2026, 9, 28)))]
    assert (run.status, run.watermark_before, run.watermark_after) == (
        "success", None, "2026-09-28")
    assert h.state.watermark == "2026-09-28"


def test_window_starts_the_day_after_the_watermark():
    h = api_harness(watermark="2026-09-20")

    h.run()

    assert h.reader.reads == [("2026-09-20", DateWindow(date(2026, 9, 21), date(2026, 9, 28)))]


def test_window_of_one_day():
    h = api_harness(watermark="2026-09-27")

    run = h.run()

    assert h.reader.reads[0][1] == DateWindow(date(2026, 9, 28), date(2026, 9, 28))
    assert run.watermark_after == "2026-09-28"


def test_up_to_date_window_succeeds_without_calling_the_source():
    h = api_harness(watermark="2026-09-28")
    before = h.state

    run = h.run()

    assert h.reader.reads == []
    assert h.writer.tables == {}
    assert h.state == before
    assert (run.status, run.watermark_after, run.error) == ("skipped", "2026-09-28", None)


def test_window_with_no_rows_still_advances_the_watermark():
    h = api_harness(watermark="2026-09-20", rows=[])

    run = h.run()

    assert h.writer.tables == {}
    assert run.watermark_after == h.state.watermark == "2026-09-28"


def test_api_without_window_is_read_as_a_snapshot():
    snapshot = Ingestion.model_validate({**WEATHER.model_dump(by_alias=True), "source": {
        "type": "api", "url": "https://api.example/v1"}})
    h = Harness(snapshot, reader=FakeReader(ROWS))

    run = h.run()

    assert h.reader.reads == [(None, None)]
    assert (run.status, run.watermark_after) == ("success", None)


def test_invalid_watermark_fails_without_calling_the_source():
    h = api_harness(watermark="not-a-date")

    with pytest.raises(ValueError):
        h.run()

    assert h.reader.reads == []
    assert h.log.runs[0].status == "failed"
