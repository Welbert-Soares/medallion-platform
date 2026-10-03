"""`medallion-catalog-structure [--apply] [CONFIG_DIR]`: print the Catalog Structure diff, and apply it.

Dry-run by default. Authenticates like the Databricks SDK (e.g. DATABRICKS_HOST/DATABRICKS_TOKEN).
"""

import argparse
import sys

from databricks.sdk import WorkspaceClient

from medallion.controller.catalog_structure import reconcile
from medallion.controller.config import load_config
from medallion.databricks.unity_catalog import SdkUnityCatalog


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="medallion-catalog-structure", description=__doc__)
    parser.add_argument("config_dir", nargs="?", default="config/platform")
    parser.add_argument("--apply", action="store_true", help="apply the plan (default: dry-run)")
    args = parser.parse_args(argv)

    structure = load_config(args.config_dir).catalog_structure
    changes = reconcile(structure, SdkUnityCatalog(WorkspaceClient()), dry_run=not args.apply)

    print(f"Catalog Structure plan ({'applied' if args.apply else 'dry-run, nothing changed'}):")
    for line in changes.describe():
        print(f"  {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
