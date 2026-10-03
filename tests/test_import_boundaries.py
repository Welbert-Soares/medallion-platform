"""ADR 0005: import boundaries of the `medallion` package, enforced by ruff."""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def ruff_violations(module_path: str, source: str) -> str:
    """Lint `source` as if it lived at `module_path`; return ruff's output ('' when clean)."""
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "--no-cache", "--output-format", "concise",
         "--ignore", "F401", "--stdin-filename", module_path, "-"],
        input=source, capture_output=True, text=True, cwd=REPO_ROOT, check=False,
    )
    return "" if result.returncode == 0 else result.stdout + result.stderr


DATABRICKS_IMPORTS = [
    "import pyspark\n",
    "from pyspark.sql import DataFrame\n",
    "import databricks.sdk\n",
    "from databricks.sdk.runtime import dbutils\n",
    "from delta.tables import DeltaTable\n",
]


@pytest.mark.parametrize("module_path", [
    "src/medallion/controller/probe.py",
    "src/medallion/ingestion/probe.py",
    "src/medallion/probe.py",
])
@pytest.mark.parametrize("source", DATABRICKS_IMPORTS)
def test_only_databricks_module_may_import_databricks_libraries(module_path, source):
    assert "TID251" in ruff_violations(module_path, source)


@pytest.mark.parametrize("source", DATABRICKS_IMPORTS)
def test_databricks_module_may_import_databricks_libraries(source):
    assert ruff_violations("src/medallion/databricks/probe.py", source) == ""


@pytest.mark.parametrize("source", [
    "from medallion.ingestion import x\n",
    "import medallion.ingestion\n",
    "from medallion import ingestion\n",
    "from ..ingestion import x\n",
    "from .. import ingestion\n",
])
def test_controller_must_not_import_ingestion(source):
    assert ruff_violations("src/medallion/controller/probe.py", source) != ""


def test_ingestion_may_import_controller():
    assert ruff_violations(
        "src/medallion/ingestion/probe.py", "from medallion.controller import x\n"
    ) == ""


def test_dbutils_is_not_a_builtin_inside_the_package():
    assert "F821" in ruff_violations("src/medallion/controller/probe.py", "dbutils.fs.ls('/')\n")


def test_notebooks_keep_databricks_builtins():
    assert ruff_violations("databricks/notebooks/probe.py", "spark.sql('select 1')\n") == ""
