"""`load_config`: the single entry point that turns the config directory into domain objects.

Layout of the config directory:

    catalog_structure.yml     Layers, Controller, Source Systems
    connections.yml           `connections:` list
    ingestions/<system>.yml   `source_system:` + `ingestions:` list, one file per Source System
"""

from pathlib import Path

import yaml
from pydantic import TypeAdapter, ValidationError

from medallion.controller.clock import Clock, SystemClock
from medallion.controller.config.macros import macro_values, resolve_macros
from medallion.controller.config.model import (
    CatalogStructure,
    Connection,
    Ingestion,
    PlatformConfig,
    duplicates,
)


class ConfigError(ValueError):
    """Every problem found in the config directory, one message per problem."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("invalid config:\n" + "\n".join(f"  - {e}" for e in errors))


def load_config(config_dir: str | Path, clock: Clock | None = None) -> PlatformConfig:
    loader = _Loader(Path(config_dir), clock or SystemClock())
    config = loader.load()
    if loader.errors:
        raise ConfigError(loader.errors)
    return config


class _Loader:
    def __init__(self, root: Path, clock: Clock):
        self.root = root
        self.macros = macro_values(clock)
        self.errors: list[str] = []

    def load(self) -> PlatformConfig | None:
        structure = self._load_catalog_structure()
        connections = self._load_connections()
        ingestions = self._load_ingestions()
        if self.errors:
            return None
        self._check_references(structure, connections, ingestions)
        if self.errors:
            return None
        return PlatformConfig(
            catalog_structure=structure, connections=connections, ingestions=ingestions
        )

    # ── files ────────────────────────────────────────────────────────────────

    def _load_catalog_structure(self) -> CatalogStructure | None:
        path = self.root / "catalog_structure.yml"
        content = self._read(path, expected_keys=None)
        if content is None:
            return None
        return self._parse(path, CatalogStructure, content)

    def _load_connections(self) -> tuple[Connection, ...]:
        path = self.root / "connections.yml"
        content = self._read(path, expected_keys={"connections"})
        if content is None:
            return ()
        return self._parse(path, tuple[Connection, ...], content.get("connections") or [], ())

    def _load_ingestions(self) -> tuple[Ingestion, ...]:
        folder = self.root / "ingestions"
        if not folder.is_dir():
            self.errors.append(f"{self._rel(folder)}: directory not found")
            return ()
        ingestions: list[Ingestion] = []
        for path in sorted(folder.glob("*.yml")):
            content = self._read(path, expected_keys={"source_system", "ingestions"})
            if content is None:
                continue
            if "source_system" not in content:
                self.errors.append(f"{self._rel(path)}: source_system: Field required")
                continue
            try:
                content = resolve_macros(content, self.macros)
            except KeyError as e:
                self.errors.append(f"{self._rel(path)}: unknown macro {{{{ {e.args[0]} }}}}")
                continue
            items = [
                {**item, "source_system": content["source_system"]}
                if isinstance(item, dict) else item
                for item in content.get("ingestions") or []
            ]
            ingestions.extend(self._parse(path, tuple[Ingestion, ...], items, ()))
        return tuple(ingestions)

    def _read(self, path: Path, expected_keys: set[str] | None) -> dict | None:
        if not path.is_file():
            self.errors.append(f"{self._rel(path)}: file not found")
            return None
        try:
            content = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            self.errors.append(f"{self._rel(path)}: invalid YAML: {e}")
            return None
        if not isinstance(content, dict):
            self.errors.append(f"{self._rel(path)}: expected a mapping at the top level")
            return None
        if expected_keys is not None and (extra := sorted(set(content) - expected_keys)):
            self.errors.append(f"{self._rel(path)}: unexpected top-level keys {extra}")
            return None
        return content

    def _parse(self, path: Path, model, content, default=None):
        try:
            return TypeAdapter(model).validate_python(content)
        except ValidationError as e:
            for err in e.errors():
                loc = ".".join(str(p) for p in err["loc"])
                self.errors.append(f"{self._rel(path)}: {loc}: {err['msg']}")
            return default

    def _rel(self, path: Path) -> str:
        return str(path.relative_to(self.root)) if path.is_relative_to(self.root) else str(path)

    # ── cross-references ─────────────────────────────────────────────────────

    def _check_references(self, structure, connections, ingestions) -> None:
        if dupes := duplicates(c.id for c in connections):
            self.errors.append(f"duplicate connection ids: {dupes}")
        if dupes := duplicates(i.name for i in ingestions):
            self.errors.append(f"duplicate ingestion names: {dupes}")
        if dupes := duplicates((i.source_system, i.raw_table) for i in ingestions):
            tables = [f"{system}.{table}" for system, table in dupes]
            self.errors.append(f"raw tables targeted by more than one ingestion: {tables}")
        connection_ids = {c.id for c in connections}
        for ingestion in ingestions:
            if structure.source_system(ingestion.source_system) is None:
                self.errors.append(
                    f"ingestion {ingestion.name}: source system "
                    f"'{ingestion.source_system}' is not declared in catalog_structure.yml"
                )
            connection_id = getattr(ingestion.source, "connection_id", None)
            if connection_id is not None and connection_id not in connection_ids:
                self.errors.append(
                    f"ingestion {ingestion.name}: connection '{connection_id}' "
                    "is not declared in connections.yml"
                )
