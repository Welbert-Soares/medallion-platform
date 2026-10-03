"""`plan(declared, actual)`: the pure diff that makes Unity Catalog match the Catalog Structure."""

from collections.abc import Mapping
from dataclasses import dataclass

from medallion.controller.catalog_structure.state import ObjectState, Securable, State


@dataclass(frozen=True)
class Create:
    securable: Securable
    comment: str | None


@dataclass(frozen=True)
class SetComment:
    securable: Securable
    comment: str


@dataclass(frozen=True)
class SetTags:
    securable: Securable
    tags: Mapping[str, str]


@dataclass(frozen=True)
class GrantChange:
    securable: Securable
    principal: str
    privileges: frozenset[str]


@dataclass(frozen=True)
class Plan:
    creates: tuple[Create, ...] = ()
    comments: tuple[SetComment, ...] = ()
    tags: tuple[SetTags, ...] = ()
    grants: tuple[GrantChange, ...] = ()
    revokes: tuple[GrantChange, ...] = ()
    # Existing objects in managed catalogs that are not declared; reported, never dropped.
    orphans: tuple[Securable, ...] = ()

    @property
    def has_changes(self) -> bool:
        return bool(self.creates or self.comments or self.tags or self.grants or self.revokes)

    def describe(self) -> list[str]:
        lines = [f"+ create {c.securable}" + (f" ({c.comment!r})" if c.comment else "")
                 for c in self.creates]
        lines += [f"~ comment {c.securable}: {c.comment!r}" for c in self.comments]
        lines += [f"~ tags {t.securable}: {dict(t.tags)}" for t in self.tags]
        lines += [f"+ grant {_privs(g)} on {g.securable} to {g.principal!r}" for g in self.grants]
        lines += [f"- revoke {_privs(r)} on {r.securable} from {r.principal!r}"
                  for r in self.revokes]
        lines += [f"! orphan {o} (not declared; left in place)" for o in self.orphans]
        return lines or ["No changes."]


def _privs(change: GrantChange) -> str:
    return ", ".join(sorted(change.privileges))


def plan(declared: State, actual: State) -> Plan:
    creates, comments, tags, grants, revokes = [], [], [], [], []

    for securable in sorted(declared, key=Securable.sort_key):
        want = declared[securable]
        have = actual.get(securable)
        if have is None:
            creates.append(Create(securable, want.comment))
            have = ObjectState(comment=want.comment, grants={})
        elif want.comment is not None and want.comment != have.comment:
            comments.append(SetComment(securable, want.comment))

        if missing := {k: v for k, v in want.tags.items() if have.tags.get(k) != v}:
            tags.append(SetTags(securable, missing))

        if want.grants is not None:
            have_grants = have.grants or {}
            for principal in sorted(set(want.grants) | set(have_grants)):
                wanted = want.grants.get(principal, frozenset())
                held = have_grants.get(principal, frozenset())
                if wanted - held:
                    grants.append(GrantChange(securable, principal, wanted - held))
                if held - wanted:
                    revokes.append(GrantChange(securable, principal, held - wanted))

    catalogs = {s.name for s in declared if s.kind == "catalog"}
    orphans = [
        s for s in sorted(actual, key=Securable.sort_key)
        if s not in declared and s.catalog in catalogs
        # report an orphan schema once, not each volume inside it
        and (s.parent is None or s.parent in declared)
    ]
    return Plan(tuple(creates), tuple(comments), tuple(tags), tuple(grants), tuple(revokes),
                tuple(orphans))
