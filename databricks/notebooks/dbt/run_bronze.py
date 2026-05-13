# Databricks notebook source
# Installs dbt-databricks and runs all bronze projects in sequence.

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

ctx       = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host      = ctx.apiUrl().get().replace("https://", "")
token     = ctx.apiToken().get()

dbutils.widgets.text("warehouse_http_path", "")
http_path = dbutils.widgets.get("warehouse_http_path")

BRONZE_PROJECTS = [
    {"name": "bronze_weather", "catalog": "medallion_bronze", "schema": "weather"},
    {"name": "bronze_ibge",    "catalog": "medallion_bronze", "schema": "ibge"},
]

# COMMAND ----------

def build_profiles(projects):
    lines = []
    for p in projects:
        lines.append(f"""
{p['name']}:
  target: prod
  outputs:
    prod:
      type: databricks
      host: {host}
      http_path: {http_path}
      token: {token}
      catalog: {p['catalog']}
      schema: {p['schema']}
      threads: 4
""")
    return "\n".join(lines)

with open(f"{PROFILES_DIR}/profiles.yml", "w") as f:
    f.write(build_profiles(BRONZE_PROJECTS))

# COMMAND ----------

errors = []
for project in BRONZE_PROJECTS:
    project_dir = os.path.join(DBT_DIR, project["name"])
    print(f"\n{'='*60}")
    print(f"Running {project['name']} → {project['catalog']}.{project['schema']}")
    print('='*60)

    deps = subprocess.run(
        [DBT_CMD, "deps", "--profiles-dir", PROFILES_DIR, "--no-use-colors"],
        cwd=project_dir, capture_output=True, text=True
    )
    print(deps.stdout)

    result = subprocess.run(
        [DBT_CMD, "run", "--profiles-dir", PROFILES_DIR, "--no-use-colors"],
        cwd=project_dir, capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        errors.append(
            f"{project['name']}:\n"
            f"--- stdout ---\n{result.stdout[-3000:]}\n"
            f"--- stderr ---\n{result.stderr[-1000:]}"
        )

if errors:
    raise Exception(f"dbt bronze failed:\n" + "\n".join(errors))

print("\ndbt bronze — all projects done.")
