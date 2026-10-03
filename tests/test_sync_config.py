"""Config sync: the Controller tables mirror the config, with no runtime state (ADR 0003)."""

import json
from datetime import date, datetime, timezone

from fakes.clock import FixedClock
from fakes.controller_tables import FakeControllerTables

from medallion.controller.config import PlatformConfig
from medallion.controller.sync import CONNECTION_COLUMNS, INGESTION_COLUMNS, sync_config

NOW = datetime(2026, 10, 3, 9, 0, tzinfo=timezone.utc)
CLOCK = FixedClock(NOW)

STRUCTURE = {
    "layers": [{"name": "raw", "catalog": "medallion_raw"}],
    "controller": {"catalog": "medallion_controller", "schema": {"name": "controller"}},
    "source_systems": [{"name": "weather"}, {"name": "ibge"}, {"name": "orders"}],
}
CONNECTION = {
    "id": "pg", "description": "demo", "subprotocol": "postgresql",
    "driver": "org.postgresql.Driver", "host": "db.example", "port": 5432, "database": "shop",
    "user": "reader", "secret_scope": "medallion-platform", "secret_key": "pg_password",
}
API = {
    "name": "weather_sao_paulo", "source_system": "weather", "raw_table": "sao_paulo",
    "description": "Clima", "active": True,
    "source": {"type": "api", "url": "https://api.example/v1", "params": {"lat": -23.5},
               "flatten_method": "zip_arrays", "data_key": "hourly",
               "window": {"start_param": "start_date", "end_param": "end_date",
                          "start_date": "2026-01-01", "lag_days": 5}},
}
CSV = {
    "name": "ibge_municipios", "source_system": "ibge", "raw_table": "municipios",
    "active": True, "source": {"type": "csv", "path": "/Volumes/x/m.csv", "delimiter": ";"},
}
SQL = {
    "name": "orders_query", "source_system": "orders", "raw_table": "orders", "active": False,
    "source": {"type": "sql", "connection_id": "pg", "query": "SELECT * FROM orders",
               "watermark_column": "updated_at", "partition_column": "order_id"},
}
TABLE = {
    "name": "orders_customers", "source_system": "orders", "raw_table": "customers",
    "active": True, "source": {"type": "table", "connection_id": "pg", "schema": "public",
                               "table": "customers", "filter": "WHERE active"},
}


def config(*ingestions, connections=(CONNECTION,)) -> PlatformConfig:
    return PlatformConfig.model_validate({
        "catalog_structure": STRUCTURE, "connections": list(connections),
        "ingestions": list(ingestions)})


def synced(cfg: PlatformConfig) -> FakeControllerTables:
    tables = FakeControllerTables()
    sync_config(cfg, tables, CLOCK)
    return tables


def rows_by_name(tables) -> dict:
    return {r["ingestion_name"]: r for r in tables.ingestions}


def test_every_row_has_exactly_the_table_columns():
    tables = synced(config(API, CSV, SQL, TABLE))

    assert all(set(r) == set(INGESTION_COLUMNS) for r in tables.ingestions)
    assert all(set(r) == set(CONNECTION_COLUMNS) for r in tables.connections)


def test_api_ingestion_row():
    row = rows_by_name(synced(config(API)))["weather_sao_paulo"]

    assert row == {
        **dict.fromkeys(INGESTION_COLUMNS),
        "ingestion_name": "weather_sao_paulo", "source_system": "weather",
        "raw_table": "medallion_raw.weather.sao_paulo", "description": "Clima", "active": True,
        "source_type": "api", "source_url": "https://api.example/v1",
        "source_params": json.dumps({"lat": -23.5}), "source_flatten_method": "zip_arrays",
        "source_data_key": "hourly", "source_window_start_param": "start_date",
        "source_window_end_param": "end_date", "source_window_start_date": date(2026, 1, 1),
        "source_window_lag_days": 5, "synced_at": NOW,
    }


def test_csv_sql_and_table_rows():
    rows = rows_by_name(synced(config(CSV, SQL, TABLE)))

    csv, sql, table = rows["ibge_municipios"], rows["orders_query"], rows["orders_customers"]
    assert (csv["source_type"], csv["source_path"], csv["source_delimiter"], csv["source_header"]) \
        == ("csv", "/Volumes/x/m.csv", ";", True)
    assert (sql["source_connection_id"], sql["source_query"], sql["source_watermark_column"],
            sql["source_partition_column"], sql["active"]) \
        == ("pg", "SELECT * FROM orders", "updated_at", "order_id", False)
    assert (table["source_schema"], table["source_table"], table["source_filter"]) \
        == ("public", "customers", "WHERE active")


def test_api_without_window_leaves_window_columns_empty():
    snapshot = {**API, "source": {"type": "api", "url": "https://api.example/v1"}}
    row = rows_by_name(synced(config(snapshot)))["weather_sao_paulo"]

    assert row["source_window_start_date"] is None
    assert row["source_params"] is None


def test_ingestion_removed_from_config_disappears_from_the_table():
    tables = FakeControllerTables()
    sync_config(config(API, CSV), tables, CLOCK)

    sync_config(config(CSV), tables, CLOCK)

    assert list(rows_by_name(tables)) == ["ibge_municipios"]


def test_connection_row_holds_address_and_secret_reference_only():
    (row,) = synced(config()).connections

    assert row == {
        "connection_id": "pg", "description": "demo", "subprotocol": "postgresql",
        "driver": "org.postgresql.Driver", "host": "db.example", "port": 5432,
        "database": "shop", "user": "reader", "secret_scope": "medallion-platform",
        "secret_key": "pg_password", "synced_at": NOW,
    }


def test_tables_hold_no_runtime_state():
    state_words = ("last_", "last_run", "updated_at", "status", "state")
    columns = set(INGESTION_COLUMNS) | set(CONNECTION_COLUMNS)

    assert not [c for c in columns if any(w in c for w in state_words)]


def test_empty_config_empties_the_tables():
    tables = synced(config(API))
    sync_config(config(connections=()), tables, CLOCK)

    assert (tables.ingestions, tables.connections) == ([], [])


def test_sync_reports_counts():
    result = sync_config(config(API, CSV), FakeControllerTables(), CLOCK)

    assert (result.ingestions, result.connections) == (2, 1)
