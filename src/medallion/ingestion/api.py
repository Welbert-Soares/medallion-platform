"""Calling a JSON API and turning its response into records; Spark-free so it is testable."""

import json
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any, Protocol

from medallion.controller.config.model import ApiSource
from medallion.ingestion.window import DateWindow


class HttpJson(Protocol):
    def get(self, url: str, params: Mapping[str, object]) -> Any: ...


class UrllibHttpJson:
    def __init__(self, timeout: float = 60):
        self.timeout = timeout

    def get(self, url: str, params: Mapping[str, object]) -> Any:
        query = urllib.parse.urlencode({k: _param(v) for k, v in params.items()})
        with urllib.request.urlopen(f"{url}?{query}", timeout=self.timeout) as response:
            return json.load(response)


def request_params(source: ApiSource, window: DateWindow | None) -> dict[str, object]:
    params: dict[str, object] = dict(source.params)
    if source.window and window:
        params[source.window.start_param] = window.start.isoformat()
        params[source.window.end_param] = window.end.isoformat()
    return params


def fetch_records(source: ApiSource, window: DateWindow | None, http: HttpJson) -> list[dict]:
    payload = http.get(source.url, request_params(source, window))
    return flatten_response(payload, source.flatten_method, source.data_key)


def flatten_response(payload: Any, method: str, data_key: str | None) -> list[dict]:
    data = payload[data_key] if data_key else payload
    if method == "zip_arrays":  # {"time": [...], "temp": [...]} → one record per position
        keys = list(data)
        return [dict(zip(keys, values)) for values in zip(*(data[k] for k in keys))]
    records = data if isinstance(data, list) else [data]
    if method == "json_normalize":
        return [_normalize(r) for r in records]
    return records


def _normalize(record: Mapping, prefix: str = "") -> dict:
    flat: dict = {}
    for key, value in record.items():
        name = f"{prefix}{key}"
        if isinstance(value, Mapping):
            flat.update(_normalize(value, f"{name}_"))
        else:
            flat[name] = value
    return flat


def _param(value: object) -> object:
    return str(value).lower() if isinstance(value, bool) else value
