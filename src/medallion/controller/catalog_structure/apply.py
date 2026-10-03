"""The `UnityCatalog` port and the use case that reconciles it with the Catalog Structure."""

from collections.abc import Iterable, Mapping
from typing import Protocol

from medallion.controller.catalog_structure.plan import Plan, plan
from medallion.controller.catalog_structure.state import (
    Securable,
    State,
    declared_state,
    managed_catalogs,
)
from medallion.controller.config.model import CatalogStructure


class UnityCatalog(Protocol):
    def read(self, catalogs: Iterable[str]) -> State:
        """Existing catalogs among `catalogs`, with all their schemas and volumes."""
        ...

    def create(self, securable: Securable, comment: str | None) -> None: ...

    def set_comment(self, securable: Securable, comment: str) -> None: ...

    def set_tags(self, securable: Securable, tags: Mapping[str, str]) -> None: ...

    def grant(self, securable: Securable, principal: str, privileges: frozenset[str]) -> None: ...

    def revoke(self, securable: Securable, principal: str, privileges: frozenset[str]) -> None: ...


def apply(changes: Plan, uc: UnityCatalog) -> None:
    """Execute a plan. Orphans are only reported: nothing is ever dropped (ADR 0004)."""
    for create in changes.creates:  # already ordered catalog → schema → volume
        uc.create(create.securable, create.comment)
    for comment in changes.comments:
        uc.set_comment(comment.securable, comment.comment)
    for tags in changes.tags:
        uc.set_tags(tags.securable, tags.tags)
    for grant in changes.grants:
        uc.grant(grant.securable, grant.principal, grant.privileges)
    for revoke in changes.revokes:
        uc.revoke(revoke.securable, revoke.principal, revoke.privileges)


def reconcile(structure: CatalogStructure, uc: UnityCatalog, *, dry_run: bool) -> Plan:
    """Diff the declared Catalog Structure against Unity Catalog; apply it unless `dry_run`."""
    changes = plan(declared_state(structure), uc.read(managed_catalogs(structure)))
    if not dry_run:
        apply(changes, uc)
    return changes
