import yaml
from cerberus import Validator


class YAMLSchemaValidator:
    def __init__(self, schema_file_path: str, content: dict):
        self.schema_file_path = schema_file_path
        self.content = content

    def _load_schema(self) -> dict:
        with open(self.schema_file_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def validate(self):
        schema = self._load_schema()
        validator = Validator(schema)
        if not validator.validate(self.content):
            raise ValueError(
                f"YAML schema validation failed for {self.schema_file_path}:\n{validator.errors}"
            )
        return validator.document
