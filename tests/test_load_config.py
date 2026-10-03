"""Seam 1: `load_config(directory)` — YAML fixtures in, domain objects or validation errors out."""

import copy
from datetime import date
from pathlib import Path

import pytest
import yaml
from fakes.clock import FixedClock

from medallion.controller.config import (
    ApiSource,
    ApiWindow,
    ConfigError,
    CsvSource,
    SqlSource,
    TableSource,
    load_config,
)
from medallion.controller.config.validate import main as validate_main

REPO_ROOT = Path(__file__).resolve().parents[1]
CLOCK = FixedClock(date(2026, 10, 3))

CATALOG_STRUCTURE = {
    "layers": [
        {"name": "raw", "catalog": "medallion_raw",
         "schemas": [{"name": "sources", "volumes": [{"name": "files"}]}]},
        {"name": "bronze", "catalog": "medallion_bronze"},
    ],
    "controller": {"catalog": "medallion_controller", "schema": {"name": "controller"}},
    "source_systems": [{"name": "weather"}, {"name": "ibge"}, {"name": "orders"}],
}
CONNECTIONS = {
    "connections": [{
        "id": "pg", "subprotocol": "postgresql", "driver": "org.postgresql.Driver",
        "host": "db.example", "port": 5432, "database": "shop", "user": "reader",
        "secret_scope": "medallion-platform", "secret_key": "pg_password",
    }],
}
API = {
    "name": "weather_sao_paulo", "raw_table": "sao_paulo", "active": True,
    "source": {"type": "api", "url": "https://api.example/v1", "params": {"lat": -23.5}},
}
CSV = {
    "name": "ibge_municipios", "raw_table": "municipios", "active": True,
    "source": {"type": "csv", "path": "/Volumes/medallion_raw/sources/files/m.csv"},
}
SQL = {
    "name": "orders_query", "raw_table": "orders", "active": False,
    "source": {"type": "sql", "connection_id": "pg", "query": "SELECT * FROM orders",
               "filter": "WHERE status = 'paid'", "watermark_column": "updated_at",
               "partition_column": "order_id"},
}
TABLE = {
    "name": "orders_customers", "raw_table": "customers", "active": True,
    "source": {"type": "table", "connection_id": "pg", "schema": "public", "table": "customers"},
}


def write_yaml(path: Path, content) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(content, allow_unicode=True), encoding="utf-8")


def write_config(root: Path, *, structure=None, connections=None, ingestions=None) -> Path:
    """Write a config directory; `ingestions` maps file stem → file content."""
    write_yaml(root / "catalog_structure.yml", structure or CATALOG_STRUCTURE)
    write_yaml(root / "connections.yml", connections or CONNECTIONS)
    if ingestions is None:
        ingestions = {
            "weather": {"source_system": "weather", "ingestions": [API]},
            "ibge": {"source_system": "ibge", "ingestions": [CSV]},
            "orders": {"source_system": "orders", "ingestions": [SQL, TABLE]},
        }
    (root / "ingestions").mkdir(parents=True, exist_ok=True)
    for stem, content in ingestions.items():
        write_yaml(root / "ingestions" / f"{stem}.yml", content)
    return root


def with_changes(base: dict, **changes) -> dict:
    result = copy.deepcopy(base)
    result.update(changes)
    return result


def errors_of(root: Path) -> str:
    with pytest.raises(ConfigError) as exc:
        load_config(root, CLOCK)
    return "\n".join(exc.value.errors)


# ── happy path: every source type ────────────────────────────────────────────


def test_loads_every_source_type_into_typed_ingestions(tmp_path):
    config = load_config(write_config(tmp_path), CLOCK)

    by_name = {i.name: i for i in config.ingestions}
    assert set(by_name) == {"weather_sao_paulo", "ibge_municipios", "orders_query",
                            "orders_customers"}

    api = by_name["weather_sao_paulo"]
    assert isinstance(api.source, ApiSource)
    assert api.source_system == "weather"
    assert api.source.params == {"lat": -23.5}
    assert api.source.flatten_method == "default"
    assert api.source.window is None

    csv = by_name["ibge_municipios"].source
    assert isinstance(csv, CsvSource)
    assert (csv.delimiter, csv.header) == (",", True)

    sql = by_name["orders_query"].source
    assert isinstance(sql, SqlSource)
    assert (sql.connection_id, sql.filter, sql.watermark_column, sql.partition_column) == (
        "pg", "WHERE status = 'paid'", "updated_at", "order_id")
    assert by_name["orders_query"].active is False

    table = by_name["orders_customers"].source
    assert isinstance(table, TableSource)
    assert (table.schema_, table.table) == ("public", "customers")


def test_ingestion_target_is_raw_schema_of_its_source_system(tmp_path):
    config = load_config(write_config(tmp_path), CLOCK)

    assert config.raw_table_of(config.ingestion("weather_sao_paulo")) == (
        "medallion_raw.weather.sao_paulo")


def test_loads_catalog_structure_and_connections(tmp_path):
    config = load_config(write_config(tmp_path), CLOCK)

    structure = config.catalog_structure
    assert [layer.name for layer in structure.layers] == ["raw", "bronze"]
    assert structure.layer("raw").schemas[0].volumes[0].name == "files"
    assert (structure.controller.catalog, structure.controller.schema_.name) == (
        "medallion_controller", "controller")
    assert config.connection("pg").secret_key == "pg_password"


def test_api_window_is_parsed(tmp_path):
    window = {"start_param": "start_date", "end_param": "end_date",
              "start_date": "2026-01-01", "lag_days": 5}
    api = with_changes(API, source={**API["source"], "window": window})
    root = write_config(tmp_path, ingestions={
        "weather": {"source_system": "weather", "ingestions": [api]}})

    source = load_config(root, CLOCK).ingestion("weather_sao_paulo").source

    assert source.window == ApiWindow(start_param="start_date", end_param="end_date",
                                      start_date=date(2026, 1, 1), lag_days=5)


def test_an_empty_ingestions_folder_is_valid(tmp_path):
    assert load_config(write_config(tmp_path, ingestions={}), CLOCK).ingestions == ()


# ── macros ───────────────────────────────────────────────────────────────────


def test_macros_are_resolved_with_the_clock(tmp_path):
    params = {"day": "{{ today }}", "prev": "{{yesterday}}",
              "range": "{{ last_year }}..{{ current_year }}", "n": 1}
    window = {"start_param": "s", "end_param": "e", "start_date": "{{ last_year }}-01-01"}
    api = with_changes(API, source={**API["source"], "params": params, "window": window})
    root = write_config(tmp_path, ingestions={
        "weather": {"source_system": "weather", "ingestions": [api]}})

    source = load_config(root, FixedClock(date(2026, 1, 1))).ingestion("weather_sao_paulo").source

    assert source.params == {"day": "2026-01-01", "prev": "2025-12-31",
                             "range": "2025..2026", "n": 1}
    assert source.window.start_date == date(2025, 1, 1)


def test_unknown_macro_is_rejected(tmp_path):
    api = with_changes(API, source={**API["source"], "params": {"d": "{{ tomorrow }}"}})
    root = write_config(tmp_path, ingestions={
        "weather": {"source_system": "weather", "ingestions": [api]}})

    assert "unknown macro {{ tomorrow }}" in errors_of(root)


# ── shape errors ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("source", "missing"), [
    ({"type": "api"}, "url"),
    ({"type": "csv"}, "path"),
    ({"type": "sql", "query": "SELECT 1"}, "connection_id"),
    ({"type": "sql", "connection_id": "pg"}, "query"),
    ({"type": "table", "connection_id": "pg", "table": "t"}, "schema"),
])
def test_each_source_type_requires_its_own_fields(tmp_path, source, missing):
    item = with_changes(API, source=source)
    root = write_config(tmp_path, ingestions={
        "weather": {"source_system": "weather", "ingestions": [item]}})

    errors = errors_of(root)

    assert "ingestions/weather.yml" in errors
    assert f".{missing}: Field required" in errors


def test_unknown_source_type_is_rejected(tmp_path):
    root = write_config(tmp_path, ingestions={"weather": {
        "source_system": "weather", "ingestions": [with_changes(API, source={"type": "ftp"})]}})

    assert "source" in errors_of(root)


@pytest.mark.parametrize("removed_field", [
    {"flag_distinct": True}, {"flag_hash_row": False}, {"flag_data_factory": False},
    {"target": {"container": "medallion_raw", "schema": "weather"}},
])
def test_removed_fields_are_rejected(tmp_path, removed_field):
    root = write_config(tmp_path, ingestions={"weather": {
        "source_system": "weather", "ingestions": [with_changes(API, **removed_field)]}})

    assert "Extra inputs are not permitted" in errors_of(root)


def test_connection_never_holds_a_password(tmp_path):
    connection = {**CONNECTIONS["connections"][0], "password": "hunter2"}
    root = write_config(tmp_path, connections={"connections": [connection]})

    assert "connections.yml: 0.password: Extra inputs are not permitted" in errors_of(root)


def test_ingestion_file_requires_source_system(tmp_path):
    root = write_config(tmp_path, ingestions={"weather": {"ingestions": [API]}})

    assert "ingestions/weather.yml: source_system: Field required" in errors_of(root)


def test_catalog_structure_requires_the_raw_layer(tmp_path):
    structure = with_changes(CATALOG_STRUCTURE, layers=[{"name": "bronze", "catalog": "b"}])

    assert "the raw layer must be declared" in errors_of(write_config(tmp_path, structure=structure))


def test_missing_config_file_is_reported(tmp_path):
    root = write_config(tmp_path)
    (root / "connections.yml").unlink()

    assert "connections.yml: file not found" in errors_of(root)


def test_all_errors_are_reported_together(tmp_path):
    root = write_config(tmp_path, ingestions={
        "weather": {"source_system": "weather", "ingestions": [with_changes(API, active="maybe")]},
        "ibge": {"ingestions": [CSV]},
    })

    errors = errors_of(root)

    assert "ingestions/weather.yml: 0.active" in errors
    assert "ingestions/ibge.yml: source_system: Field required" in errors


# ── cross-reference errors ───────────────────────────────────────────────────


def test_ingestion_with_undeclared_source_system_is_rejected(tmp_path):
    root = write_config(tmp_path, ingestions={
        "wheather": {"source_system": "wheather", "ingestions": [API]}})

    assert ("ingestion weather_sao_paulo: source system 'wheather' is not declared"
            in errors_of(root))


@pytest.mark.parametrize("item", [SQL, TABLE])
def test_sql_and_table_with_unknown_connection_are_rejected(tmp_path, item):
    item = with_changes(item, source={**item["source"], "connection_id": "missing"})
    root = write_config(tmp_path, ingestions={
        "orders": {"source_system": "orders", "ingestions": [item]}})

    assert f"ingestion {item['name']}: connection 'missing' is not declared" in errors_of(root)


def test_duplicate_ingestion_names_are_rejected_across_files(tmp_path):
    root = write_config(tmp_path, ingestions={
        "weather": {"source_system": "weather", "ingestions": [API]},
        "ibge": {"source_system": "ibge", "ingestions": [with_changes(CSV, name=API["name"])]},
    })

    assert "duplicate ingestion names: ['weather_sao_paulo']" in errors_of(root)


def test_two_ingestions_into_the_same_raw_table_are_rejected(tmp_path):
    second = with_changes(API, name="weather_rio")
    root = write_config(tmp_path, ingestions={
        "weather": {"source_system": "weather", "ingestions": [API, second]}})

    assert "['weather.sao_paulo']" in errors_of(root)


# ── the repository's config and the validation command ───────────────────────


def test_repository_config_mirrors_current_platform():
    config = load_config(REPO_ROOT / "config" / "platform", CLOCK)

    structure = config.catalog_structure
    assert {(layer.name, layer.catalog) for layer in structure.layers} == {
        ("raw", "medallion_raw"), ("bronze", "medallion_bronze"),
        ("silver", "medallion_silver"), ("gold", "medallion_gold")}
    assert (structure.controller.catalog, structure.controller.schema_.name) == (
        "medallion_controller", "controller")
    assert {s.name for s in structure.source_systems} == {"weather", "ibge", "orders"}
    assert {config.raw_table_of(i): (i.source.type, i.active) for i in config.ingestions} == {
        "medallion_raw.weather.sao_paulo": ("api", True),
        "medallion_raw.ibge.municipios": ("csv", True),
        "medallion_raw.orders.orders": ("sql", False),
    }
    weather = config.ingestion("weather_sao_paulo").source
    assert weather.url == "https://archive-api.open-meteo.com/v1/archive"
    assert (weather.window.start_date, weather.window.lag_days) == (date(2026, 1, 1), 5)


def test_validate_command_succeeds_on_valid_config(tmp_path, capsys):
    assert validate_main([str(write_config(tmp_path))]) == 0
    assert "OK" in capsys.readouterr().out


def test_validate_command_fails_on_invalid_config(tmp_path, capsys):
    root = write_config(tmp_path, ingestions={"x": {"source_system": "nope", "ingestions": [API]}})

    assert validate_main([str(root)]) == 1
    assert "source system 'nope'" in capsys.readouterr().err
