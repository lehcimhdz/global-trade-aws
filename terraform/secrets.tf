# ── Airflow Variables backed by AWS Secrets Manager ──────────────────────────
#
# Airflow reads Variables from Secrets Manager when configured with
# SecretsManagerBackend.  The path convention is:
#   airflow/variables/<VARIABLE_NAME>
#
# This file creates the secret *names* only — values are populated by the
# `make bootstrap-secrets` command (scripts/bootstrap_secrets.sh) so that
# sensitive data never enters Terraform state.
#
# Reference: https://airflow.apache.org/docs/apache-airflow-providers-amazon/
#            stable/secrets-backends/aws-secrets-manager.html

locals {
  # Sensitive variables that must live in Secrets Manager
  secret_variables = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
  ]

  # Non-sensitive pipeline config promoted to Secrets Manager for consistency
  config_variables = [
    "COMTRADE_S3_BUCKET",
    "COMTRADE_TYPE_CODE",
    "COMTRADE_FREQ_CODE",
    "COMTRADE_CL_CODE",
    "COMTRADE_WRITE_PARQUET",
    "AWS_DEFAULT_REGION",
  ]

  all_variables = concat(local.secret_variables, local.config_variables)
}

resource "aws_secretsmanager_secret" "airflow_variable" {
  for_each = toset(local.all_variables)

  name        = "airflow/variables/${each.value}"
  description = "Airflow Variable: ${each.value} (managed by Terraform, value set via bootstrap_secrets.sh)"

  # Immediate deletion in dev; 30-day recovery window in prod
  recovery_window_in_days = var.environment == "prod" ? 30 : var.secrets_recovery_window_days

  tags = {
    AirflowVariable = each.value
    Sensitive       = contains(local.secret_variables, each.value) ? "true" : "false"
  }
}
