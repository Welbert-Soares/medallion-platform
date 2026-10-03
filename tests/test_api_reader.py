"""The API request built for a window (the old reader sent the Watermark in the wrong param)."""

from datetime import date

import pytest

from medallion.controller.config import ApiSource
from medallion.ingestion.api import fetch_records, flatten_response
from medallion.ingestion.window import DateWindow


class FakeHttp:
    def __init__(self, payload):
        self.payload = payload
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.payload


ARCHIVE = ApiSource.model_validate({
    "type": "api", "url": "https://archive-api.open-meteo.com/v1/archive",
    "params": {"latitude": -23.5505, "longitude": -46.6333,
               "hourly": "temperature_2m,precipitation", "timezone": "America/Sao_Paulo"},
    "flatten_method": "zip_arrays", "data_key": "hourly",
    "window": {"start_param": "start_date", "end_param": "end_date",
               "start_date": "2026-01-01", "lag_days": 5}})
HOURLY = {"hourly": {"time": ["2026-09-27T00:00", "2026-09-27T01:00"],
                     "temperature_2m": [15.1, 14.8], "precipitation": [0.0, 0.2]}}


def test_window_is_sent_in_the_declared_params_with_the_static_params():
    http = FakeHttp(HOURLY)

    fetch_records(ARCHIVE, DateWindow(date(2026, 9, 21), date(2026, 9, 28)), http)

    assert http.calls == [("https://archive-api.open-meteo.com/v1/archive", {
        "latitude": -23.5505, "longitude": -46.6333, "hourly": "temperature_2m,precipitation",
        "timezone": "America/Sao_Paulo", "start_date": "2026-09-21", "end_date": "2026-09-28"})]


def test_snapshot_sends_only_the_static_params():
    snapshot = ApiSource.model_validate({**ARCHIVE.model_dump(), "window": None})
    http = FakeHttp(HOURLY)

    fetch_records(snapshot, None, http)

    assert http.calls[0][1] == dict(ARCHIVE.params)


def test_zip_arrays_turns_parallel_arrays_into_records():
    records = fetch_records(ARCHIVE, DateWindow(date(2026, 9, 27), date(2026, 9, 27)),
                            FakeHttp(HOURLY))

    assert records == [
        {"time": "2026-09-27T00:00", "temperature_2m": 15.1, "precipitation": 0.0},
        {"time": "2026-09-27T01:00", "temperature_2m": 14.8, "precipitation": 0.2},
    ]


@pytest.mark.parametrize(("payload", "method", "key", "expected"), [
    ({"items": [{"a": 1}, {"a": 2}]}, "default", "items", [{"a": 1}, {"a": 2}]),
    ({"a": 1}, "default", None, [{"a": 1}]),
    ([{"a": {"b": 1, "c": {"d": 2}}}], "json_normalize", None, [{"a_b": 1, "a_c_d": 2}]),
    ({"hourly": {"time": []}}, "zip_arrays", "hourly", []),
])
def test_flatten_methods(payload, method, key, expected):
    assert flatten_response(payload, method, key) == expected
