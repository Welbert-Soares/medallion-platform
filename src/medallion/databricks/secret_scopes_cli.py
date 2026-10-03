"""`medallion-secret-scopes [CONFIG_DIR]`: create the secret scopes declared Connections use.

Idempotent; never writes secret values (the maintainer puts them, see docs/connections.md).
"""

import argparse
import sys

from databricks.sdk import WorkspaceClient

from medallion.controller.config import load_config
from medallion.controller.connections import secret_scopes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="medallion-secret-scopes", description=__doc__)
    parser.add_argument("config_dir", nargs="?", default="config/platform")
    args = parser.parse_args(argv)

    w = WorkspaceClient()
    existing = {s.name for s in w.secrets.list_scopes()}
    for scope in secret_scopes(load_config(args.config_dir)):
        if scope in existing:
            print(f"secret scope {scope}: exists")
        else:
            w.secrets.create_scope(scope)
            print(f"secret scope {scope}: created")
    return 0


if __name__ == "__main__":
    sys.exit(main())
