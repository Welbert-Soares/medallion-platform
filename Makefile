# Day-to-day commands. Databricks credentials (DATABRICKS_HOST, DATABRICKS_TOKEN) come from .env.
-include .env
export

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
CONFIG ?= config/platform

.PHONY: install lint test validate plan deploy

install:  ## dev environment: package + pytest, ruff, pyspark, dbt, databricks-sdk
	$(PYTHON) -m pip install -e ".[dev]"

lint:  ## ruff, including the ADR 0005 import boundaries
	$(PYTHON) -m ruff check src/ tests/

test:  ## pytest (no Spark cluster or JVM needed)
	$(PYTHON) -m pytest

validate:  ## config (load_config) + bundle definition
	$(PYTHON) -m medallion.controller.config.validate $(CONFIG)
	databricks bundle validate --target prod

plan:  ## Catalog Structure dry-run against the workspace: prints the diff, changes nothing
	$(PYTHON) -m medallion.databricks.catalog_structure_cli $(CONFIG)

deploy:  ## what the merge to main does, minus the CSV upload: apply, secret scopes, bundle (no Run)
	$(PYTHON) -m medallion.databricks.catalog_structure_cli $(CONFIG) --apply
	$(PYTHON) -m medallion.databricks.secret_scopes_cli $(CONFIG)
	databricks bundle deploy --target prod
