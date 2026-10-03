from dataclasses import replace

from medallion.controller.catalog_structure import ObjectState, Securable, State


class FakeUnityCatalog:
    """In-memory Unity Catalog; records every write so dry-runs can be checked."""

    def __init__(self, objects: State | None = None):
        self.objects: State = dict(objects or {})
        self.writes: list[tuple] = []

    def read(self, catalogs) -> State:
        catalogs = set(catalogs)
        return {s: o for s, o in self.objects.items() if s.catalog in catalogs}

    def create(self, securable: Securable, comment) -> None:
        if securable in self.objects:
            raise ValueError(f"{securable} already exists")
        if securable.parent is not None and securable.parent not in self.objects:
            raise ValueError(f"parent of {securable} does not exist")
        self.writes.append(("create", securable))
        self.objects[securable] = ObjectState(comment=comment, grants={})

    def set_comment(self, securable, comment) -> None:
        self.writes.append(("comment", securable))
        self.objects[securable] = replace(self.objects[securable], comment=comment)

    def set_tags(self, securable, tags) -> None:
        self.writes.append(("tags", securable))
        current = self.objects[securable]
        self.objects[securable] = replace(current, tags={**current.tags, **tags})

    def grant(self, securable, principal, privileges) -> None:
        self._change_grants(securable, principal, lambda held: held | privileges)

    def revoke(self, securable, principal, privileges) -> None:
        self._change_grants(securable, principal, lambda held: held - privileges)

    def _change_grants(self, securable, principal, change) -> None:
        self.writes.append(("grants", securable))
        current = self.objects[securable]
        grants = dict(current.grants or {})
        grants[principal] = change(grants.get(principal, frozenset()))
        if not grants[principal]:
            del grants[principal]
        self.objects[securable] = replace(current, grants=grants)
