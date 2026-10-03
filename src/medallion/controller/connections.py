"""Connections as consumed at run time; credentials are only ever read from a secret store."""

from typing import Protocol

from medallion.controller.config.model import Connection, PlatformConfig


class ConnectionCatalog(Protocol):
    def get(self, connection_id: str) -> Connection:
        """Raises KeyError for an undeclared Connection."""
        ...


class SecretProvider(Protocol):
    def get(self, scope: str, key: str) -> str: ...


class ConfigConnectionCatalog:
    """Connections from the config deployed with the bundle."""

    def __init__(self, config: PlatformConfig):
        self._connections = {c.id: c for c in config.connections}

    def get(self, connection_id: str) -> Connection:
        try:
            return self._connections[connection_id]
        except KeyError:
            raise KeyError(f"connection '{connection_id}' is not declared in config") from None


def secret_scopes(config: PlatformConfig) -> list[str]:
    """Secret scopes the declared Connections read their passwords from."""
    return sorted({c.secret_scope for c in config.connections})
