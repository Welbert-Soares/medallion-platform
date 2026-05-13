# Databricks notebook source
# Installs dbt-databricks and runs gold models.

# COMMAND ----------

# MAGIC %pip install dbt-databricks "protobuf>=5.26.1,<6.0.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import subprocess, os, shutil, sys

DBT_CMD = os.path.join(os.path.dirname(sys.executable), "dbt")

DBT_SRC      = "/Workspace/Repos/welbertsoares6@gmail.com/medallion-platform/dbt"
DBT_DIR      = "/tmp/medallion-dbt"
PROFILES_DIR = "/tmp/medallion-profiles"

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
      catalog: medallion_gold
      schema: weather
      threads: 4
"""
with open(f"{PROFILES_DIR}/profiles.yml", "w") as f:
    f.write(profiles_yml)

# COMMAND ----------

result = subprocess.run(
    [DBT_CMD, "run", "--select", "tag:gold",
     "--profiles-dir", PROFILES_DIR, "--no-use-colors"],
    cwd=DBT_DIR, capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    raise Exception(
        f"dbt gold failed (exit {result.returncode})\n"
        f"--- stdout ---\n{result.stdout[-3000:]}\n"
        f"--- stderr ---\n{result.stderr[-2000:]}"
    )

print("dbt gold — done.")
