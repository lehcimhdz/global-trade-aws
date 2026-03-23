#!/usr/bin/env bash
# trigger_backfill.sh — convenience wrapper for the comtrade_backfill DAG
#
# Usage:
#   ./scripts/trigger_backfill.sh \
#       --endpoint preview \
#       --periods  2020,2021,2022
#
#   ./scripts/trigger_backfill.sh \
#       --endpoint preview \
#       --periods  2020,2021 \
#       --type-code C \
#       --freq-code A \
#       --cl-code   HS \
#       --reporter  842 \
#       --partner   156 \
#       --flow-code M
#
# The script builds the dag_run.conf JSON and triggers the DAG via
# `airflow dags trigger`.  It uses `docker compose exec` by default;
# set AIRFLOW_CMD to override (e.g. for MWAA/K8s environments).
#
# AIRFLOW_CMD examples:
#   docker compose exec airflow-webserver airflow   (default)
#   airflow                                          (local install)
#   kubectl exec -n airflow deploy/airflow-web -- airflow

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

ENDPOINT=""
PERIODS=""
TYPE_CODE="C"
FREQ_CODE="A"
CL_CODE="HS"
REPORTER_CODE=""
PARTNER_CODE=""
CMD_CODE=""
FLOW_CODE=""
AIRFLOW_CMD="${AIRFLOW_CMD:-docker compose exec airflow-webserver airflow}"

# ── Argument parsing ──────────────────────────────────────────────────────────

usage() {
    cat <<EOF
Usage: $(basename "$0") --endpoint ENDPOINT --periods PERIOD1,PERIOD2,... [OPTIONS]

Required:
  --endpoint   Comtrade endpoint: preview | previewTariffline | getMBS
  --periods    Comma-separated periods, e.g. 2020,2021,2022

Optional:
  --type-code  Commodity type code (default: C)
  --freq-code  Frequency code: A (annual) | M (monthly)  (default: A)
  --cl-code    Classification code, e.g. HS (default: HS)
  --reporter   Reporter country code(s)
  --partner    Partner country code(s)
  --cmd-code   Commodity code(s)
  --flow-code  Flow code: X (export) | M (import)

Environment:
  AIRFLOW_CMD  Override the Airflow CLI command
               (default: "docker compose exec airflow-webserver airflow")

Examples:
  # Backfill US annual preview data for 2019-2022
  $0 --endpoint preview --periods 2019,2020,2021,2022 --reporter 842

  # Backfill monthly MBS data for 2023
  $0 --endpoint getMBS --periods 202301,202302,202303 --freq-code M
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --endpoint)   ENDPOINT="$2";       shift 2 ;;
        --periods)    PERIODS="$2";        shift 2 ;;
        --type-code)  TYPE_CODE="$2";      shift 2 ;;
        --freq-code)  FREQ_CODE="$2";      shift 2 ;;
        --cl-code)    CL_CODE="$2";        shift 2 ;;
        --reporter)   REPORTER_CODE="$2";  shift 2 ;;
        --partner)    PARTNER_CODE="$2";   shift 2 ;;
        --cmd-code)   CMD_CODE="$2";       shift 2 ;;
        --flow-code)  FLOW_CODE="$2";      shift 2 ;;
        --help|-h)    usage ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

if [[ -z "$ENDPOINT" ]] || [[ -z "$PERIODS" ]]; then
    echo "Error: --endpoint and --periods are required." >&2
    usage
fi

# ── Build JSON conf via Python (portable, handles quoting correctly) ──────────

CONF=$(python3 - <<PYEOF
import json, sys

periods = "$PERIODS".split(",")
conf = {
    "endpoint":  "$ENDPOINT",
    "periods":   periods,
    "type_code": "$TYPE_CODE",
    "freq_code": "$FREQ_CODE",
    "cl_code":   "$CL_CODE",
}
for key, val in [
    ("reporter_code", "$REPORTER_CODE"),
    ("partner_code",  "$PARTNER_CODE"),
    ("cmd_code",      "$CMD_CODE"),
    ("flow_code",     "$FLOW_CODE"),
]:
    if val:
        conf[key] = val

print(json.dumps(conf, indent=2))
PYEOF
)

# ── Trigger the DAG ───────────────────────────────────────────────────────────

echo ""
echo "Triggering comtrade_backfill with conf:"
echo "$CONF"
echo ""

# shellcheck disable=SC2086
$AIRFLOW_CMD dags trigger comtrade_backfill \
    --conf "$(echo "$CONF" | tr -d '\n')"

echo ""
echo "DAG triggered.  Monitor progress at http://localhost:8080/dags/comtrade_backfill"
