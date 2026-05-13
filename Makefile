.PHONY: setup test compile run docs clean upload sync-config

setup:
	pip install -r requirements.txt
	@if [ ! -f ~/.dbt/profiles.yml ]; then \
	  cp dbt/profiles.yml.example ~/.dbt/profiles.yml; \
	  echo "⚠  Edit ~/.dbt/profiles.yml with your Databricks credentials"; \
	fi
	cd dbt && dbt deps

compile:
	cd dbt && dbt compile

test:
	cd dbt && dbt compile && dbt test

docs:
	cd dbt && dbt docs generate && dbt docs serve

upload:
	@source .env && \
	databricks workspace import_dir databricks/notebooks /Shared/medallion-platform --overwrite && \
	databricks fs cp -r config/ dbfs:/FileStore/medallion-platform/config --overwrite && \
	databricks fs cp -r dbt/ dbfs:/FileStore/medallion-platform/dbt --overwrite && \
	databricks fs cp data/sources/ibge_municipios.csv dbfs:/FileStore/medallion-platform/sources/ibge_municipios.csv --overwrite

sync-config:
	@echo "Trigger sync_config via Databricks job one-off run..."
	@source .env && databricks jobs run-now --job-id $$DATABRICKS_JOB_ID

run:
	@source .env && databricks jobs run-now --job-id $$DATABRICKS_JOB_ID

clean:
	cd dbt && dbt clean
