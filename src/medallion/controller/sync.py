"""Config sync: mirror the declared Ingestions and Connections into the Controller tables.

Both tables are fully replaced on every sync and hold no runtime state (ADR 0003): an Ingestion
removed from YAML disappears from the table, and Watermarks live in `ingestion_state` instead.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from medallion.controller.clock import Clock
from medallion.controller.config.model import Connection, Ingestion, PlatformConfig

# Column → Spark SQL type. Source fields are flattened as `source_<field>`; `params` is JSON.
INGESTION_COLUMNS: dict[str, str] = {
    "ingestion_name": "STRING NOT NULL",
    "source_system": "STRING NOT NULL",
    "raw_table": "STRING NOT NULL",
    "description": "STRING",
    "active": "BOOLEAN NOT NULL",
    "source_type": "STRING NOT NULL",
    # api
    "source_url": "STRING",
    "source_params": "STRING",
    "source_flatten_method": "STRING",
    "source_data_key": "STRING",
    "source_window_start_param": "STRING",
    "source_window_end_param": "STRING",
    "source_window_start_date": "DATE",
    "source_window_lag_days": "INT",
    # csv
    "source_path": "STRING",
    "source_delimiter": "STRING",
    "source_header": "BOOLEAN",
    # sql / table
    "source_connection_id": "STRING",
    "source_query": "STRING",
    "source_schema": "STRING",
    "source_table": "STRING",
    "source_filter": "STRING",
    "source_watermark_column": "STRING",
    "source_partition_column": "STRING",
    "synced_at": "TIMESTAMP NOT NULL",
}

# Address and secret reference only: the password never leaves the secret scope.
CONNECTION_COLUMNS: dict[str, str] = {
    "connection_id": "STRING NOT NULL",
    "description": "STRING",
    "subprotocol": "STRING NOT NULL",
    "driver": "STRING NOT NULL",
    "host": "STRING NOT NULL",
    "port": "INT NOT NULL",
    "database": "STRING NOT NULL",
    "user": "STRING NOT NULL",
    "secret_scope": "STRING NOT NULL",
    "secret_key": "STRING NOT NULL",
    "synced_at": "TIMESTAMP NOT NULL",
}

_JSON_KEYS = {"params"}

Row = dict[str, object]


class ControllerTables(Protocol):
    """Writes the config mirror tables of the Controller schema."""

    def replace_ingestions(self, rows: list[Row]) -> None: ...

    def replace_connections(self, rows: list[Row]) -> None: ...


@dataclass(frozen=True)
class SyncResult:
    ingestions: int
    connections: int


def sync_config(config: PlatformConfig, tables: ControllerTables, clock: Clock) -> SyncResult:
    synced_at = clock.now()
    ingestions = [ingestion_row(config, i, synced_at) for i in config.ingestions]
    connections = [connection_row(c, synced_at) for c in config.connections]
    tables.replace_ingestions(ingestions)
    tables.replace_connections(connections)
    return SyncResult(len(ingestions), len(connections))


def ingestion_row(config: PlatformConfig, ingestion: Ingestion, synced_at: datetime) -> Row:
    source = _flatten(ingestion.source.model_dump(by_alias=True), prefix="source_")
    if unknown := sorted(set(source) - set(INGESTION_COLUMNS)):
        raise ValueError(f"source fields without a column in the ingestions table: {unknown}")
    row: Row = dict.fromkeys(INGESTION_COLUMNS)
    row.update(source)
    row.update(
        ingestion_name=ingestion.name,
        source_system=ingestion.source_system,
        raw_table=config.raw_table_of(ingestion),
        description=ingestion.description,
        active=ingestion.active,
        synced_at=synced_at,
    )
    return row


def connection_row(connection: Connection, synced_at: datetime) -> Row:
    fields = connection.model_dump()
    row: Row = {"connection_id": fields.pop("id"), **fields, "synced_at": synced_at}
    if set(row) != set(CONNECTION_COLUMNS):
        raise ValueError(f"connection fields do not match the connections table: {sorted(row)}")
    return row


def _flatten(d: dict, prefix: str) -> Row:
    flat: Row = {}
    for key, value in d.items():
        name = f"{prefix}{key}"
        if key in _JSON_KEYS:
            flat[name] = json.dumps(value) if value else None
        elif isinstance(value, dict):
            flat.update(_flatten(value, prefix=f"{name}_"))
        elif value is None and key == "window":
            continue
        else:
            flat[name] = value
    return flat
