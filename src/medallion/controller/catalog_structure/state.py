"""Unity Catalog objects as plain values, so declared and actual state can be compared."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Literal

from medallion.controller.config.model import CatalogStructure, Grant, Schema

Kind = Literal["catalog", "schema", "volume"]
_KIND_ORDER = {"catalog": 0, "schema": 1, "volume": 2}


@dataclass(frozen=True)
class Securable:
    kind: Kind
    name: str  # full name: catalog | catalog.schema | catalog.schema.volume

    @property
    def catalog(self) -> str:
        return self.name.split(".")[0]

    @property
    def parent(self) -> "Securable | None":
        if self.kind == "catalog":
            return None
        kind = "catalog" if self.kind == "schema" else "schema"
        return Securable(kind, self.name.rsplit(".", 1)[0])

    def sort_key(self) -> tuple:
        return (_KIND_ORDER[self.kind], self.name)

    def __str__(self) -> str:
        return f"{self.kind} {self.name}"


Grants = Mapping[str, frozenset[str]]  # principal → privileges


@dataclass(frozen=True)
class ObjectState:
    """Comment, tags and grants of one object.

    In declared state, `comment=None` and `grants=None` mean "not managed": the plan leaves
    whatever exists alone. Tags are managed per declared key; other keys are left alone.
    """

    comment: str | None = None
    tags: Mapping[str, str] = field(default_factory=dict)
    grants: Grants | None = None


State = dict[Securable, ObjectState]


def normalize_privilege(privilege: str) -> str:
    return privilege.strip().upper().replace(" ", "_")


def to_grants(grants: Iterable[Grant]) -> dict[str, frozenset[str]]:
    result: dict[str, set[str]] = {}
    for grant in grants:
        result.setdefault(grant.principal, set()).update(
            normalize_privilege(p) for p in grant.privileges
        )
    return {principal: frozenset(privs) for principal, privs in result.items()}


def declared_state(structure: CatalogStructure) -> State:
    """Every object the Catalog Structure declares; each Source System is a schema in every Layer."""
    state: State = {}

    def add_schema(catalog: str, schema: Schema) -> None:
        full_name = f"{catalog}.{schema.name}"
        state[Securable("schema", full_name)] = ObjectState(
            schema.comment, dict(schema.tags), to_grants(schema.grants)
        )
        for volume in schema.volumes:
            state[Securable("volume", f"{full_name}.{volume.name}")] = ObjectState(volume.comment)

    for layer in structure.layers:
        state[Securable("catalog", layer.catalog)] = ObjectState(
            layer.comment, dict(layer.tags), to_grants(layer.grants)
        )
        for system in structure.source_systems:
            state[Securable("schema", f"{layer.catalog}.{system.name}")] = ObjectState(
                system.comment, dict(system.tags), {}
            )
        for schema in layer.schemas:
            add_schema(layer.catalog, schema)

    controller = structure.controller
    state[Securable("catalog", controller.catalog)] = ObjectState(
        controller.comment, dict(controller.tags), to_grants(controller.grants)
    )
    add_schema(controller.catalog, controller.schema_)
    return state


def managed_catalogs(structure: CatalogStructure) -> list[str]:
    return [layer.catalog for layer in structure.layers] + [structure.controller.catalog]
