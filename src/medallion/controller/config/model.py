"""Domain objects of the platform config (see CONTEXT.md for the vocabulary)."""

from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

# Names that become Unity Catalog schemas/tables or keys in the Controller tables.
Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]*$")]
NonEmpty = Annotated[str, StringConstraints(min_length=1)]
ParamValue = str | int | float | bool


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


def duplicates(values) -> list:
    seen, dupes = set(), []
    for v in values:
        if v in seen and v not in dupes:
            dupes.append(v)
        seen.add(v)
    return dupes


# ── Catalog Structure ────────────────────────────────────────────────────────


class Grant(_Model):
    principal: NonEmpty
    privileges: tuple[NonEmpty, ...] = Field(min_length=1)


class Volume(_Model):
    name: Identifier
    comment: str | None = None


class Schema(_Model):
    name: Identifier
    comment: str | None = None
    tags: dict[str, str] = {}
    grants: tuple[Grant, ...] = ()
    volumes: tuple[Volume, ...] = ()


class SourceSystem(_Model):
    """Its name is the schema name in every Layer."""

    name: Identifier
    comment: str | None = None
    tags: dict[str, str] = {}


LayerName = Literal["raw", "bronze", "silver", "gold"]


class Layer(_Model):
    name: LayerName
    catalog: Identifier
    comment: str | None = None
    tags: dict[str, str] = {}
    grants: tuple[Grant, ...] = ()
    # Schemas that are not Source Systems (e.g. raw.sources holding the CSV volume).
    schemas: tuple[Schema, ...] = ()


class Controller(_Model):
    catalog: Identifier
    schema_: Schema = Field(alias="schema")
    comment: str | None = None
    tags: dict[str, str] = {}
    grants: tuple[Grant, ...] = ()

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema_.name}.{name}"


class CatalogStructure(_Model):
    layers: tuple[Layer, ...] = Field(min_length=1)
    controller: Controller
    source_systems: tuple[SourceSystem, ...] = ()

    @model_validator(mode="after")
    def _check(self):
        errors = []
        layer_names = [layer.name for layer in self.layers]
        if dupes := duplicates(layer_names):
            errors.append(f"duplicate layers: {dupes}")
        if "raw" not in layer_names:
            errors.append("the raw layer must be declared")
        catalogs = [layer.catalog for layer in self.layers] + [self.controller.catalog]
        if dupes := duplicates(catalogs):
            errors.append(f"catalogs used more than once: {dupes}")
        systems = [s.name for s in self.source_systems]
        if dupes := duplicates(systems):
            errors.append(f"duplicate source systems: {dupes}")
        for layer in self.layers:
            names = [s.name for s in layer.schemas]
            if dupes := duplicates(names):
                errors.append(f"layer {layer.name}: duplicate schemas {dupes}")
            if clash := sorted(set(names) & set(systems)):
                errors.append(f"layer {layer.name}: schemas {clash} clash with source systems")
        if errors:
            raise ValueError("; ".join(errors))
        return self

    def layer(self, name: LayerName) -> Layer:
        return next(layer for layer in self.layers if layer.name == name)

    def source_system(self, name: str) -> SourceSystem | None:
        return next((s for s in self.source_systems if s.name == name), None)


# ── Connections ──────────────────────────────────────────────────────────────


class Connection(_Model):
    """Address and secret reference of an external database; never the password itself."""

    id: Identifier
    description: str | None = None
    subprotocol: Literal["postgresql", "mysql", "sqlserver", "oracle"]
    driver: NonEmpty
    host: NonEmpty
    port: int = Field(gt=0, lt=65536)
    database: NonEmpty
    user: NonEmpty
    secret_scope: NonEmpty
    secret_key: NonEmpty


# ── Ingestions ───────────────────────────────────────────────────────────────


class ApiWindow(_Model):
    """Incremental date window sent to an API; without one the API is read as a snapshot."""

    start_param: NonEmpty
    end_param: NonEmpty
    start_date: date
    lag_days: int = Field(default=0, ge=0)


class ApiSource(_Model):
    type: Literal["api"]
    url: NonEmpty
    params: dict[str, ParamValue] = {}
    flatten_method: Literal["default", "zip_arrays", "json_normalize"] = "default"
    data_key: str | None = None
    window: ApiWindow | None = None


class CsvSource(_Model):
    type: Literal["csv"]
    path: NonEmpty
    delimiter: NonEmpty = ","
    header: bool = True


class _JdbcRead(_Model):
    connection_id: Identifier
    filter: str | None = None
    watermark_column: str | None = None
    partition_column: str | None = None


class SqlSource(_JdbcRead):
    type: Literal["sql"]
    query: NonEmpty


class TableSource(_JdbcRead):
    type: Literal["table"]
    schema_: NonEmpty = Field(alias="schema")
    table: NonEmpty


Source = Annotated[ApiSource | CsvSource | SqlSource | TableSource, Field(discriminator="type")]


class Ingestion(_Model):
    """One source object landing in Raw at `{raw catalog}.{source_system}.{raw_table}`."""

    name: Identifier
    source_system: Identifier
    raw_table: Identifier
    description: str | None = None
    active: bool
    source: Source


# ── Whole config ─────────────────────────────────────────────────────────────


class PlatformConfig(_Model):
    catalog_structure: CatalogStructure
    connections: tuple[Connection, ...] = ()
    ingestions: tuple[Ingestion, ...] = ()

    def ingestion(self, name: str) -> Ingestion:
        return next(i for i in self.ingestions if i.name == name)

    def connection(self, connection_id: str) -> Connection:
        return next(c for c in self.connections if c.id == connection_id)

    def raw_table_of(self, ingestion: Ingestion) -> str:
        raw = self.catalog_structure.layer("raw")
        return f"{raw.catalog}.{ingestion.source_system}.{ingestion.raw_table}"
