#!/usr/bin/env bash
# Import all Databricks notebooks from the local repo into the Workspace via
# the REST API (/api/2.0/workspace/import).  This preserves the notebook type
# (SOURCE format), unlike the workspace-files API which creates plain files.
#
# Usage:  ./scripts/import_notebooks.sh
# Requires: databricks CLI authenticated (databricks auth token)

set -euo pipefail

TOKEN=$(databricks auth token --profile DEFAULT 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
HOST="https://dbc-b27007cb-ae01.cloud.databricks.com"
REPO_PATH="/Workspace/Repos/welbertsoares6@gmail.com/medallion-platform"
LOCAL_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

import_nb() {
  local local_file="$1"     # absolute local path to .py notebook
  local remote_nb="$2"      # workspace path WITHOUT .py extension
  CONTENT=$(base64 -w 0 "$local_file")
  RESULT=$(curl -s -X POST "$HOST/api/2.0/workspace/import" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"path\":\"$remote_nb\",\"format\":\"SOURCE\",\"language\":\"PYTHON\",\"content\":\"$CONTENT\",\"overwrite\":true}")
  echo "  $remote_nb → $(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print('OK id=' + str(d.get('object_id','')) if 'object_id' in d else d.get('error_code','ERR') + ': ' + d.get('message',''))")"
}

echo "Importing notebooks to $REPO_PATH ..."
import_nb "$LOCAL_ROOT/databricks/notebooks/setup/00_create_tables.py"  "$REPO_PATH/databricks/notebooks/setup/00_create_tables"
import_nb "$LOCAL_ROOT/databricks/notebooks/setup/sync_config.py"        "$REPO_PATH/databricks/notebooks/setup/sync_config"
import_nb "$LOCAL_ROOT/databricks/notebooks/ingestion/controller.py"     "$REPO_PATH/databricks/notebooks/ingestion/controller"
import_nb "$LOCAL_ROOT/databricks/notebooks/ingestion/generic_ingestion.py" "$REPO_PATH/databricks/notebooks/ingestion/generic_ingestion"
import_nb "$LOCAL_ROOT/databricks/notebooks/dbt/run_silver.py"           "$REPO_PATH/databricks/notebooks/dbt/run_silver"
import_nb "$LOCAL_ROOT/databricks/notebooks/dbt/run_gold.py"             "$REPO_PATH/databricks/notebooks/dbt/run_gold"
echo "Done."
