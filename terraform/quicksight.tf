# ── QuickSight BI layer ───────────────────────────────────────────────────────
#
# Provisions the data foundation for QuickSight dashboards on top of the
# Comtrade silver Iceberg tables:
#
#   1. aws_quicksight_data_source   — Athena connection via the project workgroup
#   2. aws_quicksight_data_set      — SPICE import of reporter_summary
#   3. aws_quicksight_data_set      — SPICE import of trade_flows
#   4. aws_iam_role / policy        — QuickSight service role for Athena + S3 + Glue
#
# All resources are gated on var.enable_quicksight (default: false).
# To activate: terraform apply -var="enable_quicksight=true" \
#                              -var="quicksight_username=<your-qs-user>"
#
# After apply, log into the QuickSight console and build analyses / dashboards
# from the two published datasets.  See docs/operations.md for the step-by-step.

data "aws_caller_identity" "current" {}

locals {
  qs_principal = "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:user/default/${var.quicksight_username}"
}


# ── IAM service role ──────────────────────────────────────────────────────────

resource "aws_iam_role" "quicksight" {
  count       = var.enable_quicksight ? 1 : 0
  name        = "${local.name_prefix}-quicksight"
  description = "Allows QuickSight to query Athena, read S3, and inspect Glue tables"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "quicksight.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "quicksight_data_access" {
  count = var.enable_quicksight ? 1 : 0
  name  = "comtrade-data-access"
  role  = aws_iam_role.quicksight[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:BatchGetQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetQueryResultsStream",
          "athena:ListQueryExecutions",
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = [aws_athena_workgroup.comtrade.arn]
      },
      {
        Sid    = "S3DataLakeRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      },
      {
        Sid    = "S3AthenaResultsWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:AbortMultipartUpload",
        ]
        Resource = ["${aws_s3_bucket.data_lake.arn}/athena-results/*"]
      },
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/comtrade*",
          "arn:aws:glue:*:*:table/comtrade*/*",
        ]
      },
    ]
  })
}


# ── Athena data source ────────────────────────────────────────────────────────

resource "aws_quicksight_data_source" "athena" {
  count = var.enable_quicksight ? 1 : 0

  aws_account_id = data.aws_caller_identity.current.account_id
  data_source_id = "${local.name_prefix}-athena"
  name           = "Comtrade Athena (${var.environment})"
  type           = "ATHENA"

  parameters {
    athena {
      work_group = aws_athena_workgroup.comtrade.name
    }
  }

  ssl_properties {
    disable_ssl = false
  }

  permission {
    actions = [
      "quicksight:DescribeDataSource",
      "quicksight:DescribeDataSourcePermissions",
      "quicksight:PassDataSource",
    ]
    principal = local.qs_principal
  }

  tags = local.tags
}


# ── Dataset: reporter_summary ─────────────────────────────────────────────────
#
# SPICE import — cached in QuickSight for sub-second dashboard rendering.
# Filtered to annual data (freq_code = 'A') which is the primary use case.

resource "aws_quicksight_data_set" "reporter_summary" {
  count = var.enable_quicksight ? 1 : 0

  aws_account_id = data.aws_caller_identity.current.account_id
  data_set_id    = "${local.name_prefix}-reporter-summary"
  name           = "Comtrade Reporter Summary (${var.environment})"
  import_mode    = "SPICE"

  physical_table_map {
    physical_table_map_id = "reporter-summary"

    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      name            = "reporter_summary"

      sql_query = <<-SQL
        SELECT
            period,
            freq_code,
            reporter_code,
            reporter_iso,
            reporter_name,
            export_value_usd,
            import_value_usd,
            total_trade_value_usd,
            trade_balance_usd,
            commodity_count,
            partner_count
        FROM comtrade_silver.reporter_summary
        WHERE freq_code = 'A'
      SQL

      columns { name = "period";               type = "STRING"  }
      columns { name = "freq_code";            type = "STRING"  }
      columns { name = "reporter_code";        type = "INTEGER" }
      columns { name = "reporter_iso";         type = "STRING"  }
      columns { name = "reporter_name";        type = "STRING"  }
      columns { name = "export_value_usd";     type = "DECIMAL" }
      columns { name = "import_value_usd";     type = "DECIMAL" }
      columns { name = "total_trade_value_usd"; type = "DECIMAL" }
      columns { name = "trade_balance_usd";    type = "DECIMAL" }
      columns { name = "commodity_count";      type = "INTEGER" }
      columns { name = "partner_count";        type = "INTEGER" }
    }
  }

  permission {
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
    ]
    principal = local.qs_principal
  }

  tags = local.tags
}


# ── Dataset: trade_flows ──────────────────────────────────────────────────────
#
# SPICE import of commodity-level bilateral flows.
# Annual data only to keep the SPICE size manageable; monthly data can be
# queried live via DIRECT_QUERY mode by changing import_mode below.

resource "aws_quicksight_data_set" "trade_flows" {
  count = var.enable_quicksight ? 1 : 0

  aws_account_id = data.aws_caller_identity.current.account_id
  data_set_id    = "${local.name_prefix}-trade-flows"
  name           = "Comtrade Trade Flows (${var.environment})"
  import_mode    = "SPICE"

  physical_table_map {
    physical_table_map_id = "trade-flows"

    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      name            = "trade_flows"

      sql_query = <<-SQL
        SELECT
            period,
            freq_code,
            type_code,
            reporter_code,
            reporter_iso,
            reporter_name,
            partner_code,
            partner_iso,
            partner_name,
            commodity_code,
            commodity_name,
            flow_code,
            flow_name,
            trade_value_usd,
            source_record_count
        FROM comtrade_silver.trade_flows
        WHERE freq_code = 'A'
      SQL

      columns { name = "period";              type = "STRING"  }
      columns { name = "freq_code";           type = "STRING"  }
      columns { name = "type_code";           type = "STRING"  }
      columns { name = "reporter_code";       type = "INTEGER" }
      columns { name = "reporter_iso";        type = "STRING"  }
      columns { name = "reporter_name";       type = "STRING"  }
      columns { name = "partner_code";        type = "INTEGER" }
      columns { name = "partner_iso";         type = "STRING"  }
      columns { name = "partner_name";        type = "STRING"  }
      columns { name = "commodity_code";      type = "STRING"  }
      columns { name = "commodity_name";      type = "STRING"  }
      columns { name = "flow_code";           type = "STRING"  }
      columns { name = "flow_name";           type = "STRING"  }
      columns { name = "trade_value_usd";     type = "DECIMAL" }
      columns { name = "source_record_count"; type = "INTEGER" }
    }
  }

  permission {
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
    ]
    principal = local.qs_principal
  }

  tags = local.tags
}
