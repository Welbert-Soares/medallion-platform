#!/usr/bin/env bash
# Import notebooks via REST API, then sync the Databricks Repo from GitHub.
#
# The workspace/import API leaves the Repo's git working tree dirty, which
# blocks `repos update`.  The correct flow is:
#   1. Push changes to GitHub
#   2. Delete any stale notebook objects from the Repo workspace
#   3. Run `repos update` to pull the latest commit
#
# This script is for ONE-OFF emergency deploys when you need a notebook
# running in Databricks before pushing to GitHub (e.g. mid-debug).
# Normal deploy path: git push → deploy.yml (GitHub Actions) → repos update.
#
# Usage:  ./scripts/import_notebooks.sh [--sync-only]
#   --sync-only   Skip imports, just delete stale notebooks and pull from GitHub
# Requires: databricks CLI authenticated (databricks auth token)

set -euo pipefail

TOKEN=$(databricks auth token --profile DEFAULT 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
HOST="https://dbc-b27007cb-ae01.cloud.databricks.com"
REPO_ID="1895247745249042"
REPO_PATH="/Workspace/Repos/welbertsoares6@gmail.com/medallion-platform"
LOCAL_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

delete_nb() {
  curl -s -X POST "$HOST/api/2.0/workspace/delete" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"path\":\"$1\",\"recursive\":false}" > /dev/null
}

import_nb() {
  local local_file="$1"
  local remote_nb="$2"
  # Delete first so the git working tree is clean before import
  delete_nb "$remote_nb"
  CONTENT=$(base64 -w 0 "$local_file")
  RESULT=$(curl -s -X POST "$HOST/api/2.0/workspace/import" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"path\":\"$remote_nb\",\"format\":\"SOURCE\",\"language\":\"PYTHON\",\"content\":\"$CONTENT\",\"overwrite\":true}")
  echo "  $remote_nb → $(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print('OK' if 'object_id' in d else d)")"
}

sync_repo() {
  echo "Syncing Repo $REPO_ID to latest GitHub main ..."
  # Delete stale notebook objects so repos update can fast-forward cleanly
  NOTEBOOKS=(
    "$REPO_PATH/databricks/notebooks/setup/00_create_tables"
    "$REPO_PATH/databricks/notebooks/setup/sync_config"
    "$REPO_PATH/databricks/notebooks/ingestion/controller"
    "$REPO_PATH/databricks/notebooks/ingestion/generic_ingestion"
    "$REPO_PATH/databricks/notebooks/dbt/run_silver"
    "$REPO_PATH/databricks/notebooks/dbt/run_gold"
  )
  for nb in "${NOTEBOOKS[@]}"; do delete_nb "$nb"; done
  databricks repos update "$REPO_ID" --branch main
  echo "Repo synced."
}

if [[ "${1:-}" == "--sync-only" ]]; then
  sync_repo
  exit 0
fi

echo "Importing notebooks to $REPO_PATH ..."
import_nb "$LOCAL_ROOT/databricks/notebooks/setup/00_create_tables.py"        "$REPO_PATH/databricks/notebooks/setup/00_create_tables"
import_nb "$LOCAL_ROOT/databricks/notebooks/setup/sync_config.py"             "$REPO_PATH/databricks/notebooks/setup/sync_config"
import_nb "$LOCAL_ROOT/databricks/notebooks/ingestion/controller.py"          "$REPO_PATH/databricks/notebooks/ingestion/controller"
import_nb "$LOCAL_ROOT/databricks/notebooks/ingestion/generic_ingestion.py"   "$REPO_PATH/databricks/notebooks/ingestion/generic_ingestion"
import_nb "$LOCAL_ROOT/databricks/notebooks/dbt/run_silver.py"               "$REPO_PATH/databricks/notebooks/dbt/run_silver"
import_nb "$LOCAL_ROOT/databricks/notebooks/dbt/run_gold.py"                 "$REPO_PATH/databricks/notebooks/dbt/run_gold"
echo "Done. Run 'git push && ./scripts/import_notebooks.sh --sync-only' to re-sync the Repo."
