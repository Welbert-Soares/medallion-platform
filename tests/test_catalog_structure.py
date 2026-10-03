"""Seam 3: Catalog Structure `plan(declared, actual)` and `apply` against a fake Unity Catalog."""

from datetime import date
from pathlib import Path

from fakes.clock import FixedClock
from fakes.unity_catalog import FakeUnityCatalog

from medallion.controller.catalog_structure import (
    GrantChange,
    ObjectState,
    Securable,
    declared_state,
    plan,
    reconcile,
)
from medallion.controller.config import CatalogStructure, load_config

REPO_ROOT = Path(__file__).resolve().parents[1]
BROWSE = {"principal": "account users", "privileges": ["BROWSE"]}

STRUCTURE = CatalogStructure.model_validate({
    "layers": [
        {"name": "raw", "catalog": "raw", "comment": "Raw layer", "grants": [BROWSE],
         "schemas": [{"name": "sources", "volumes": [{"name": "files", "comment": "CSVs"}]}]},
        {"name": "bronze", "catalog": "bronze", "tags": {"layer": "bronze"}},
    ],
    "controller": {"catalog": "ctl", "schema": {
        "name": "controller", "grants": [{"principal": "admins", "privileges": ["use schema"]}]}},
    "source_systems": [{"name": "weather", "comment": "Weather"}, {"name": "ibge"}],
})


def catalog(name):
    return Securable("catalog", name)


def schema(name):
    return Securable("schema", name)


def volume(name):
    return Securable("volume", name)


def existing_catalogs(overrides: dict | None = None) -> dict:
    """The catalogs exist (they are created by hand), as they are before the first apply."""
    objects = {catalog(c): ObjectState(grants={}) for c in ("raw", "bronze", "ctl")}
    objects.update(overrides or {})
    return objects


# ── declared state ───────────────────────────────────────────────────────────


def test_each_source_system_is_a_schema_in_every_layer():
    declared = declared_state(STRUCTURE)

    assert {s.name for s in declared if s.kind == "schema"} == {
        "raw.weather", "raw.ibge", "raw.sources", "bronze.weather", "bronze.ibge",
        "ctl.controller"}
    assert {s.name for s in declared if s.kind == "volume"} == {"raw.sources.files"}
    assert declared[schema("bronze.weather")].comment == "Weather"


# ── creation ─────────────────────────────────────────────────────────────────


def test_apply_on_empty_unity_catalog_creates_everything_parents_first():
    uc = FakeUnityCatalog()

    reconcile(STRUCTURE, uc, dry_run=False)

    assert set(uc.objects) == set(declared_state(STRUCTURE))
    assert uc.objects[volume("raw.sources.files")].comment == "CSVs"
    assert uc.objects[catalog("raw")].grants == {"account users": {"BROWSE"}}
    assert uc.objects[catalog("bronze")].tags == {"layer": "bronze"}


def test_missing_schemas_are_created_with_their_comment():
    changes = plan(declared_state(STRUCTURE), existing_catalogs())

    created = {c.securable: c.comment for c in changes.creates}
    assert created[schema("raw.weather")] == "Weather"
    assert schema("ctl.controller") in created
    assert catalog("raw") not in created


# ── grants ───────────────────────────────────────────────────────────────────


def test_declared_grant_that_is_missing_is_granted():
    changes = plan(declared_state(STRUCTURE), existing_catalogs())

    assert GrantChange(catalog("raw"), "account users", frozenset({"BROWSE"})) in changes.grants


def test_undeclared_grants_on_managed_catalogs_and_schemas_are_revoked():
    uc = FakeUnityCatalog(existing_catalogs({
        catalog("raw"): ObjectState(grants={"account users": frozenset({"BROWSE", "SELECT"}),
                                            "someone@x.com": frozenset({"USE_CATALOG"})}),
        schema("raw.weather"): ObjectState(grants={"someone@x.com": frozenset({"SELECT"})}),
    }))

    changes = reconcile(STRUCTURE, uc, dry_run=False)

    assert set(changes.revokes) == {
        GrantChange(catalog("raw"), "account users", frozenset({"SELECT"})),
        GrantChange(catalog("raw"), "someone@x.com", frozenset({"USE_CATALOG"})),
        GrantChange(schema("raw.weather"), "someone@x.com", frozenset({"SELECT"})),
    }
    assert uc.objects[catalog("raw")].grants == {"account users": {"BROWSE"}}
    assert uc.objects[schema("raw.weather")].grants == {}


def test_privileges_are_compared_in_canonical_form():
    actual = existing_catalogs({
        schema("ctl.controller"): ObjectState(grants={"admins": frozenset({"USE_SCHEMA"})})})

    changes = plan(declared_state(STRUCTURE), actual)

    assert not [g for g in changes.grants + changes.revokes
                if g.securable == schema("ctl.controller")]


def test_volume_grants_are_not_managed():
    actual = existing_catalogs({
        schema("raw.sources"): ObjectState(grants={}),
        volume("raw.sources.files"): ObjectState("CSVs", grants={"x": frozenset({"READ_VOLUME"})}),
    })

    changes = plan(declared_state(STRUCTURE), actual)

    assert not [g for g in changes.revokes if g.securable.kind == "volume"]


# ── comments and tags ────────────────────────────────────────────────────────


def test_changed_comment_is_updated_and_undeclared_comment_is_left_alone():
    actual = existing_catalogs({
        catalog("raw"): ObjectState("old comment", grants={}),
        catalog("bronze"): ObjectState("hand-written comment", grants={}),
    })

    changes = plan(declared_state(STRUCTURE), actual)

    updated = {c.securable: c.comment for c in changes.comments}
    assert updated[catalog("raw")] == "Raw layer"
    assert catalog("bronze") not in updated


def test_declared_tags_are_set_and_other_tags_left_alone():
    uc = FakeUnityCatalog(existing_catalogs({
        catalog("bronze"): ObjectState(tags={"layer": "old", "team": "data"}, grants={})}))

    reconcile(STRUCTURE, uc, dry_run=False)

    assert uc.objects[catalog("bronze")].tags == {"layer": "bronze", "team": "data"}


# ── orphans ──────────────────────────────────────────────────────────────────


def test_undeclared_objects_are_reported_as_orphans_and_never_dropped():
    uc = FakeUnityCatalog(existing_catalogs({
        schema("ctl.delta_controller"): ObjectState(grants={}),
        volume("ctl.delta_controller.old"): ObjectState(),
        schema("raw.sources"): ObjectState(grants={}),
        volume("raw.sources.stray"): ObjectState(),
        catalog("samples"): ObjectState(grants={}),
        schema("samples.nyctaxi"): ObjectState(grants={}),
    }))
    before = set(uc.objects)

    changes = reconcile(STRUCTURE, uc, dry_run=False)

    assert changes.orphans == (schema("ctl.delta_controller"), volume("raw.sources.stray"))
    assert before <= set(uc.objects)


# ── idempotence and dry-run ──────────────────────────────────────────────────


def test_second_apply_has_no_changes():
    uc = FakeUnityCatalog(existing_catalogs({
        catalog("raw"): ObjectState("old", {"layer": "x"},
                                    {"intruder": frozenset({"ALL_PRIVILEGES"})}),
        schema("ctl.delta_controller"): ObjectState(grants={}),
    }))
    assert reconcile(STRUCTURE, uc, dry_run=False).has_changes

    uc.writes.clear()
    second = reconcile(STRUCTURE, uc, dry_run=False)

    assert not second.has_changes
    assert uc.writes == []
    assert second.orphans == (schema("ctl.delta_controller"),)


def test_dry_run_reports_the_diff_without_changing_anything():
    uc = FakeUnityCatalog(existing_catalogs({
        catalog("raw"): ObjectState(grants={"intruder": frozenset({"SELECT"})})}))
    before = dict(uc.objects)

    changes = reconcile(STRUCTURE, uc, dry_run=True)

    assert changes.has_changes
    assert uc.writes == []
    assert uc.objects == before
    lines = changes.describe()
    assert "+ create schema raw.weather ('Weather')" in lines
    assert "- revoke SELECT on catalog raw from 'intruder'" in lines
    assert "+ grant BROWSE on catalog raw to 'account users'" in lines


def test_empty_plan_describes_itself():
    uc = FakeUnityCatalog()
    reconcile(STRUCTURE, uc, dry_run=False)

    assert reconcile(STRUCTURE, uc, dry_run=True).describe() == ["No changes."]


# ── the repository's Catalog Structure against the workspace today ───────────


def test_repository_grants_reproduce_current_access():
    """Snapshot of the workspace on 2026-10-03: owner + `account users: BROWSE` on every catalog."""
    config = load_config(REPO_ROOT / "config" / "platform", FixedClock(date(2026, 10, 3)))
    structure = config.catalog_structure
    browse = {"account users": frozenset({"BROWSE"})}
    current = {catalog(c): ObjectState(grants=browse) for c in (
        "medallion_raw", "medallion_bronze", "medallion_silver", "medallion_gold",
        "medallion_controller")}
    for name in ("medallion_raw.weather", "medallion_raw.ibge", "medallion_raw.orders",
                 "medallion_raw.sources", "medallion_bronze.weather", "medallion_bronze.ibge",
                 "medallion_silver.weather", "medallion_silver.ibge", "medallion_gold.weather",
                 "medallion_controller.delta_controller"):
        current[schema(name)] = ObjectState(grants={})
    current[volume("medallion_raw.sources.files")] = ObjectState(
        "Static source files (CSV, JSON) for ingestion")

    changes = plan(declared_state(structure), current)

    assert changes.grants == ()
    assert changes.revokes == ()
    assert changes.orphans == (schema("medallion_controller.delta_controller"),)
