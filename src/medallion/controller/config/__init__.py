"""Platform config: YAML directory → typed domain objects via `load_config`."""

from medallion.controller.config.loader import ConfigError, load_config
from medallion.controller.config.model import (
    ApiSource,
    ApiWindow,
    CatalogStructure,
    Connection,
    Controller,
    CsvSource,
    Grant,
    Ingestion,
    Layer,
    PlatformConfig,
    Schema,
    SourceSystem,
    SqlSource,
    TableSource,
    Volume,
)

__all__ = [
    "ApiSource",
    "ApiWindow",
    "CatalogStructure",
    "ConfigError",
    "Connection",
    "Controller",
    "CsvSource",
    "Grant",
    "Ingestion",
    "Layer",
    "PlatformConfig",
    "Schema",
    "SourceSystem",
    "SqlSource",
    "TableSource",
    "Volume",
    "load_config",
]
