import json
from .yaml_schema_validator import YAMLSchemaValidator
from .macro_resolver import resolve_all

# Keys whose dict values are serialized to JSON instead of recursively flattened.
# This prevents API query params (e.g., latitude, longitude) from becoming individual columns.
_JSON_SERIALIZE_KEYS = {"params"}


class ExtractController:
    def __init__(self, schema_file_path: str, content: dict):
        self.schema_file_path = schema_file_path
        self.content = resolve_all(content)

    def _flatten_dict(self, d: dict, parent_key: str = "", sep: str = "_") -> dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict) and k in _JSON_SERIALIZE_KEYS:
                items.append((new_key, json.dumps(v)))
            elif isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_ingestions(self) -> list[dict]:
        YAMLSchemaValidator(self.schema_file_path, self.content).validate()

        ingestions = []
        for source in self.content.get("ingestions", []):
            source_flat = self._flatten_dict(
                {k: v for k, v in source.items() if k != "tables"}
            )
            for table in source.get("tables", []):
                table_flat = self._flatten_dict(table)

                row = dict(source_flat)
                row.update({f"table_{k}": v for k, v in table_flat.items()})

                # Normalise keys expected by Delta schema
                row["ingestion_name"] = row.pop("table_name", table.get("name"))
                row["source_name"]    = row.pop("name", source.get("name"))

                ingestions.append(row)

        return ingestions

    def get_connections(self) -> list[dict]:
        YAMLSchemaValidator(self.schema_file_path, self.content).validate()

        connections = []
        for conn in self.content.get("connections", []):
            connections.append(self._flatten_dict(conn))
        return connections
