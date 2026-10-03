"""Entry point of the bundle's smoke task: proves the wheel is importable on serverless."""

import medallion
from medallion import controller, databricks, ingestion


def main() -> None:
    modules = ", ".join(m.__name__ for m in (controller, ingestion, databricks))
    print(f"medallion {medallion.__version__} OK: {modules}")
