# ── Lake Formation access control ─────────────────────────────────────────────
#
# Implements column-level access control on the Glue Data Catalog tables that
# back the silver Iceberg layer (trade_flows, reporter_summary).
#
# Design
# ------
# * The data lake S3 bucket is registered as a Lake Formation data location,
#   delegating S3 access control from IAM to Lake Formation.
# * LF tags classify the database and tables by environment and sensitivity so
#   future policy changes can be made tag-wide instead of table-by-table.
# * Principals (Airflow IAM role, MWAA execution role, API Lambda, QuickSight)
#   receive the minimum permissions required for their role:
#     - Airflow / MWAA : full SELECT + ALTER on all columns (runs dbt)
#     - API Lambda     : SELECT on public columns only (no internal codes)
#     - QuickSight     : SELECT on all columns (reads both silver tables)
#
# Column-level grants on trade_flows restrict the API Lambda to the seven
# user-facing columns, excluding internal numeric codes and provenance fields
# (reporter_code, partner_code, type_code, flow_name, source_record_count).
#
# Notes
# -----
# * Lake Formation requires that the IAM principal calling Terraform has the
#   "Lake Formation administrator" permission (or is the data lake settings admin).
# * After applying, remove the s3:* direct-IAM grants from iam.tf if you want
#   Lake Formation to be the sole access-control path (recommended for prod).
# ─────────────────────────────────────────────────────────────────────────────

# ── Data location registration ────────────────────────────────────────────────

resource "aws_lakeformation_resource" "data_lake" {
  arn = aws_s3_bucket.data_lake.arn
}

# ── LF tags ───────────────────────────────────────────────────────────────────

resource "aws_lakeformation_lf_tag" "env" {
  key    = "env"
  values = ["dev", "staging", "prod"]
}

resource "aws_lakeformation_lf_tag" "classification" {
  key    = "classification"
  values = ["public", "internal", "restricted"]
}

# ── Tag the Glue database ─────────────────────────────────────────────────────

resource "aws_lakeformation_lf_tags_lf_tag_association" "database" {
  database {
    name = aws_glue_catalog_database.comtrade.name
  }

  lf_tag {
    key    = aws_lakeformation_lf_tag.env.key
    values = [var.environment]
  }

  lf_tag {
    key    = aws_lakeformation_lf_tag.classification.key
    values = ["internal"]
  }
}

# ── Database-level DESCRIBE permission ───────────────────────────────────────

resource "aws_lakeformation_permissions" "airflow_database" {
  principal = aws_iam_role.airflow.arn

  database {
    name = aws_glue_catalog_database.comtrade.name
  }

  permissions = ["DESCRIBE"]
}

# ── trade_flows — full access for Airflow ─────────────────────────────────────

resource "aws_lakeformation_permissions" "airflow_trade_flows" {
  principal = aws_iam_role.airflow.arn

  table {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "trade_flows"
  }

  permissions = ["SELECT", "DESCRIBE", "ALTER"]
}

# ── reporter_summary — full access for Airflow ────────────────────────────────

resource "aws_lakeformation_permissions" "airflow_reporter_summary" {
  principal = aws_iam_role.airflow.arn

  table {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "reporter_summary"
  }

  permissions = ["SELECT", "DESCRIBE", "ALTER"]
}

# ── MWAA execution role — same as Airflow ─────────────────────────────────────

resource "aws_lakeformation_permissions" "mwaa_trade_flows" {
  count     = var.enable_mwaa ? 1 : 0
  principal = aws_iam_role.mwaa[0].arn

  table {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "trade_flows"
  }

  permissions = ["SELECT", "DESCRIBE", "ALTER"]
}

resource "aws_lakeformation_permissions" "mwaa_reporter_summary" {
  count     = var.enable_mwaa ? 1 : 0
  principal = aws_iam_role.mwaa[0].arn

  table {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "reporter_summary"
  }

  permissions = ["SELECT", "DESCRIBE", "ALTER"]
}

# ── API Lambda — column-level SELECT on trade_flows (public columns only) ─────
#
# Excludes internal numeric codes and provenance fields that the public API
# does not surface: reporter_code, partner_code, type_code, flow_name,
# source_record_count.

resource "aws_lakeformation_permissions" "api_trade_flows" {
  count     = var.enable_api ? 1 : 0
  principal = aws_iam_role.api_lambda[0].arn

  table_with_columns {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "trade_flows"
    column_names  = [
      "period",
      "freq_code",
      "reporter_iso",
      "reporter_name",
      "partner_iso",
      "partner_name",
      "commodity_code",
      "commodity_name",
      "flow_code",
      "trade_value_usd",
    ]
  }

  permissions = ["SELECT", "DESCRIBE"]
}

resource "aws_lakeformation_permissions" "api_reporter_summary" {
  count     = var.enable_api ? 1 : 0
  principal = aws_iam_role.api_lambda[0].arn

  table {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "reporter_summary"
  }

  permissions = ["SELECT", "DESCRIBE"]
}

# ── QuickSight — full SELECT on both silver tables ────────────────────────────

resource "aws_lakeformation_permissions" "quicksight_trade_flows" {
  count     = var.enable_quicksight ? 1 : 0
  principal = aws_iam_role.quicksight[0].arn

  table {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "trade_flows"
  }

  permissions = ["SELECT", "DESCRIBE"]
}

resource "aws_lakeformation_permissions" "quicksight_reporter_summary" {
  count     = var.enable_quicksight ? 1 : 0
  principal = aws_iam_role.quicksight[0].arn

  table {
    database_name = aws_glue_catalog_database.comtrade.name
    name          = "reporter_summary"
  }

  permissions = ["SELECT", "DESCRIBE"]
}
