# Databricks notebook source
# Installs dbt-databricks and runs silver models.

# COMMAND ----------

# MAGIC %pip install dbt-databricks --quiet

# COMMAND ----------

import subprocess, os, shutil

# dbt project is uploaded to DBFS during deploy
DBT_SRC      = "/Workspace/Repos/welbertsoares6@gmail.com/medallion-platform/dbt"
DBT_DIR      = "/tmp/medallion-dbt"
PROFILES_DIR = "/tmp/medallion-profiles"

# Copia para /tmp pois /Workspace é read-only para artefatos do dbt (target/)
if os.path.exists(DBT_DIR):
    shutil.rmtree(DBT_DIR)
shutil.copytree(DBT_SRC, DBT_DIR)
os.makedirs(PROFILES_DIR, exist_ok=True)

# COMMAND ----------

ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host  = ctx.apiUrl().get().replace("https://", "")
token = ctx.apiToken().get()

dbutils.widgets.text("warehouse_http_path", "")
http_path = dbutils.widgets.get("warehouse_http_path")

profiles_yml = f"""medallion:
  target: prod
  outputs:
    prod:
      type: databricks
      host: {host}
      http_path: {http_path}
      token: {token}
      catalog: cli_catalog
      schema: silver
      threads: 4
"""
with open(f"{PROFILES_DIR}/profiles.yml", "w") as f:
    f.write(profiles_yml)

# COMMAND ----------

result = subprocess.run(
    ["python", "-m", "dbt", "run", "--select", "tag:silver",
     "--profiles-dir", PROFILES_DIR, "--no-use-colors"],
    cwd=DBT_DIR, capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    print(result.stderr)
    raise Exception(f"dbt silver failed (exit {result.returncode})")

print("dbt silver — done.")
