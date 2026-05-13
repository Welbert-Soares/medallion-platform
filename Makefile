.PHONY: setup compile test docs clean

setup:
	pip install -r requirements.txt
	@if [ ! -f ~/.dbt/profiles.yml ]; then \
	  cp dbt/profiles.yml.example ~/.dbt/profiles.yml; \
	  echo "Edit ~/.dbt/profiles.yml with your Databricks credentials"; \
	fi

compile:
	cd dbt && dbt compile

test:
	cd dbt && dbt compile && dbt test

docs:
	cd dbt && dbt docs generate && dbt docs serve

clean:
	cd dbt && dbt clean
