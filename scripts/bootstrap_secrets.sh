#!/usr/bin/env bash
# bootstrap_secrets.sh — Populate AWS Secrets Manager with values from .env
#
# Usage:
#   ./scripts/bootstrap_secrets.sh [dev|prod]
#
# Prerequisites:
#   - .env file exists and is populated
#   - AWS CLI configured (aws configure) or environment credentials set
#   - Terraform has already been applied (secrets exist in Secrets Manager)
#
# What it does:
#   Reads each variable listed in MANAGED_SECRETS from .env and writes the
#   value to the corresponding Secrets Manager path (airflow/variables/<NAME>).
#   Sensitive variables are written as SecretString; the script never prints
#   their values to stdout.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$REPO_ROOT/.env"
ENV="${1:-dev}"

# ── Validation ────────────────────────────────────────────────────────────────

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env file not found at $ENV_FILE"
  echo "       Copy .env.example to .env and fill in the values first."
  exit 1
fi

if ! command -v aws &>/dev/null; then
  echo "ERROR: AWS CLI not found. Install it: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
  exit 1
fi

# ── Variables to push to Secrets Manager ─────────────────────────────────────

MANAGED_SECRETS=(
  "AWS_ACCESS_KEY_ID"
  "AWS_SECRET_ACCESS_KEY"
  "COMTRADE_S3_BUCKET"
  "COMTRADE_TYPE_CODE"
  "COMTRADE_FREQ_CODE"
  "COMTRADE_CL_CODE"
  "COMTRADE_WRITE_PARQUET"
  "AWS_DEFAULT_REGION"
)

SENSITIVE_SECRETS=(
  "AWS_ACCESS_KEY_ID"
  "AWS_SECRET_ACCESS_KEY"
)

# ── Source .env (safe — only extracts known keys) ─────────────────────────────

declare -A env_values

while IFS='=' read -r key value; do
  # Skip comments and empty lines
  [[ "$key" =~ ^#.*$ ]] && continue
  [[ -z "$key" ]] && continue
  # Strip surrounding quotes from value
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"
  env_values["$key"]="$value"
done < "$ENV_FILE"

# ── Push each secret ──────────────────────────────────────────────────────────

echo "Environment: $ENV"
echo "Pushing ${#MANAGED_SECRETS[@]} variables to AWS Secrets Manager..."
echo ""

REGION="${env_values[AWS_DEFAULT_REGION]:-us-east-1}"
SUCCESS=0
SKIPPED=0
FAILED=0

for var_name in "${MANAGED_SECRETS[@]}"; do
  secret_path="airflow/variables/${var_name}"
  value="${env_values[$var_name]:-}"

  if [[ -z "$value" ]]; then
    echo "  SKIP  $secret_path  (not set in .env)"
    ((SKIPPED++)) || true
    continue
  fi

  # Check if secret exists
  if ! aws secretsmanager describe-secret \
    --secret-id "$secret_path" \
    --region "$REGION" \
    --output text &>/dev/null; then
    echo "  ERROR $secret_path  (secret not found — run 'make tf-apply ENV=$ENV' first)"
    ((FAILED++)) || true
    continue
  fi

  # Push value (suppress output for sensitive secrets)
  if [[ " ${SENSITIVE_SECRETS[*]} " == *" $var_name "* ]]; then
    aws secretsmanager put-secret-value \
      --secret-id "$secret_path" \
      --secret-string "$value" \
      --region "$REGION" \
      --output text >/dev/null
    echo "  OK    $secret_path  (value hidden)"
  else
    aws secretsmanager put-secret-value \
      --secret-id "$secret_path" \
      --secret-string "$value" \
      --region "$REGION" \
      --output text >/dev/null
    echo "  OK    $secret_path  = $value"
  fi

  ((SUCCESS++)) || true
done

echo ""
echo "Done: $SUCCESS pushed, $SKIPPED skipped, $FAILED failed."

if [[ $FAILED -gt 0 ]]; then
  exit 1
fi
