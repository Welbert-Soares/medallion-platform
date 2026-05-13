#!/usr/bin/env python3
"""Valida os YAMLs de config/controller/ contra os schemas Cerberus.
Usado no CI para bloquear PRs com config inválida.
Exit code 0 = tudo OK, 1 = alguma validação falhou.
"""
import os
import sys
import yaml
from cerberus import Validator

REPO_ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CTRL_DIR      = os.path.join(REPO_ROOT, "config", "controller")
SCHEMAS_DIR   = os.path.join(CTRL_DIR, "schemas")


def load_schema(name: str) -> dict:
    with open(os.path.join(SCHEMAS_DIR, name), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate(yaml_path: str, schema: dict) -> tuple[bool, object]:
    with open(yaml_path, "r", encoding="utf-8") as f:
        content = yaml.safe_load(f)
    v = Validator(schema)
    return v.validate(content), v.errors


def main() -> int:
    failed = False

    # ── Ingestion YAMLs ──────────────────────────────────────────────────────
    ingestion_schema = load_schema("ingestions.yml")
    ingestion_files = sorted(
        f for f in os.listdir(CTRL_DIR)
        if f.endswith((".yml", ".yaml"))
        and not f.startswith("connections")
        and os.path.isfile(os.path.join(CTRL_DIR, f))
    )

    print(f"Validating {len(ingestion_files)} ingestion YAML(s)...")
    for fname in ingestion_files:
        ok, errors = validate(os.path.join(CTRL_DIR, fname), ingestion_schema)
        status = "OK  " if ok else "FAIL"
        print(f"  [{status}] {fname}" + (f": {errors}" if not ok else ""))
        if not ok:
            failed = True

    # ── connections.yml ───────────────────────────────────────────────────────
    conn_path = os.path.join(CTRL_DIR, "connections.yml")
    if os.path.exists(conn_path):
        print("Validating connections.yml...")
        ok, errors = validate(conn_path, load_schema("connections.yml"))
        status = "OK  " if ok else "FAIL"
        print(f"  [{status}] connections.yml" + (f": {errors}" if not ok else ""))
        if not ok:
            failed = True

    if failed:
        print("\nValidation FAILED — corrija os erros acima antes de fazer merge.")
        return 1

    print("\nAll configs valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
