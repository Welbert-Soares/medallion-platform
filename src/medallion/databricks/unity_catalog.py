"""`UnityCatalog` adapter over the Databricks SDK (REST; no SQL warehouse needed)."""

from collections.abc import Iterable, Mapping

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, NotFound, ResourceAlreadyExists
from databricks.sdk.service.catalog import (
    EntityTagAssignment,
    PermissionsChange,
    Privilege,
    VolumeType,
)

from medallion.controller.catalog_structure import ObjectState, Securable, State

_TAG_ENTITY_TYPES = {"catalog": "catalogs", "schema": "schemas", "volume": "volumes"}
_SYSTEM_SCHEMAS = {"information_schema"}


class SdkUnityCatalog:
    def __init__(self, client: WorkspaceClient):
        self.w = client

    def read(self, catalogs: Iterable[str]) -> State:
        state: State = {}
        for name in catalogs:
            try:
                info = self.w.catalogs.get(name)
            except NotFound:
                continue
            catalog = Securable("catalog", name)
            state[catalog] = ObjectState(info.comment, self._tags(catalog), self._grants(catalog))
            for schema in self.w.schemas.list(catalog_name=name):
                if schema.name in _SYSTEM_SCHEMAS:
                    continue
                sec = Securable("schema", schema.full_name)
                state[sec] = ObjectState(schema.comment, self._tags(sec), self._grants(sec))
                for volume in self.w.volumes.list(catalog_name=name, schema_name=schema.name):
                    vol = Securable("volume", volume.full_name)
                    # Volume grants are not managed by the Catalog Structure, so not read.
                    state[vol] = ObjectState(volume.comment, self._tags(vol))
        return state

    def create(self, securable: Securable, comment: str | None) -> None:
        parts = securable.name.split(".")
        if securable.kind == "catalog":
            self.w.catalogs.create(parts[0], comment=comment)
        elif securable.kind == "schema":
            self.w.schemas.create(parts[1], parts[0], comment=comment)
        else:
            self.w.volumes.create(*parts, VolumeType.MANAGED, comment=comment)

    def set_comment(self, securable: Securable, comment: str) -> None:
        api = {"catalog": self.w.catalogs, "schema": self.w.schemas, "volume": self.w.volumes}
        api[securable.kind].update(securable.name, comment=comment)

    def set_tags(self, securable: Securable, tags: Mapping[str, str]) -> None:
        entity_type = _TAG_ENTITY_TYPES[securable.kind]
        for key, value in tags.items():
            assignment = EntityTagAssignment(
                entity_name=securable.name, entity_type=entity_type, tag_key=key, tag_value=value
            )
            try:
                self.w.entity_tag_assignments.create(assignment)
            except (AlreadyExists, ResourceAlreadyExists):
                self.w.entity_tag_assignments.update(
                    entity_type, securable.name, key, assignment, update_mask="tag_value"
                )

    def grant(self, securable: Securable, principal: str, privileges: frozenset[str]) -> None:
        change = PermissionsChange(principal=principal, add=[Privilege(p) for p in privileges])
        self.w.grants.update(securable.kind, securable.name, changes=[change])

    def revoke(self, securable: Securable, principal: str, privileges: frozenset[str]) -> None:
        change = PermissionsChange(principal=principal, remove=[Privilege(p) for p in privileges])
        self.w.grants.update(securable.kind, securable.name, changes=[change])

    def _tags(self, securable: Securable) -> dict[str, str]:
        entity_type = _TAG_ENTITY_TYPES[securable.kind]
        return {
            t.tag_key: t.tag_value or ""
            for t in self.w.entity_tag_assignments.list(entity_type, securable.name)
        }

    def _grants(self, securable: Securable) -> dict[str, frozenset[str]]:
        response = self.w.grants.get(securable.kind, securable.name)
        return {
            a.principal: frozenset(p.value for p in a.privileges or [])
            for a in response.privilege_assignments or []
        }
