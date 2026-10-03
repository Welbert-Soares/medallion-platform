# Deploy with Databricks Asset Bundles and a `medallion` wheel

Python logic is packaged as a `medallion` wheel and deployed, together with the job definition, by Databricks Asset Bundles (`databricks.yml`). This replaces the Repos checkout + `sys.path` imports, `main_workflow.json` and the SDK job upsert in `deploy.yml`. We chose it because it versions the job with the code, gives notebooks plain `from medallion ...` imports, and is the officially supported deploy path; the cost is lock-in to the bundle CLI.
