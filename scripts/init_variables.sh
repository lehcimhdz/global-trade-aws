#!/usr/bin/env bash
# Import Airflow Variables from config/airflow_variables.json into the running stack.
# Usage: ./scripts/init_variables.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
VARS_FILE="$REPO_ROOT/config/airflow_variables.json"

echo "Importing Airflow Variables from $VARS_FILE ..."
docker compose -f "$REPO_ROOT/docker-compose.yml" exec airflow-webserver \
  airflow variables import /opt/airflow/airflow_variables.json

echo "Done. Variables imported successfully."
