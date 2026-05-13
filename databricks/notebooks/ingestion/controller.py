# Databricks notebook source
# Reads active ingestions from the controller table and runs each in parallel threads.

# COMMAND ----------

import threading

INGESTION_NOTEBOOK = "/Shared/medallion-platform/ingestion/generic_ingestion"
TIMEOUT_SEC = 3600

# COMMAND ----------

active = spark.sql("""
    SELECT ingestion_name
    FROM cli_catalog.controller.ingestions
    WHERE table_active = true
    ORDER BY ingestion_name
""").collect()

print(f"Active ingestions ({len(active)}): {[r.ingestion_name for r in active]}")

# COMMAND ----------

results = {}
errors  = {}

def _run(name):
    try:
        out = dbutils.notebook.run(INGESTION_NOTEBOOK, TIMEOUT_SEC, {"ingestion_name": name})
        results[name] = out
    except Exception as e:
        errors[name] = str(e)

threads = [threading.Thread(target=_run, args=(r.ingestion_name,), daemon=True) for r in active]
for t in threads:
    t.start()
for t in threads:
    t.join()

# COMMAND ----------

print("\n=== Ingestion Results ===")
for name, result in results.items():
    print(f"  ✓ {name}: {result}")
for name, err in errors.items():
    print(f"  ✗ {name}: {err}")

if errors:
    raise Exception(f"Failed ingestions: {list(errors.keys())}")

print(f"\nAll {len(results)} ingestion(s) completed successfully.")
