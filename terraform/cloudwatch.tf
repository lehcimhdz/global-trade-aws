# ── CloudWatch dashboard — Comtrade Pipeline ──────────────────────────────────
#
# Ops-facing view of pipeline metrics across ingestion and transformation:
#
# Ingestion metrics (emitted by validate_bronze, dim: DagId + Endpoint):
#   RowCount         — data volume per DAG run
#   ChecksPassed     — quality gate pass count
#   ChecksFailed     — quality gate fail count  (also drives failure-rate widget)
#   JsonBytesWritten — raw S3 data volume
#
# dbt transformation metrics (emitted by comtrade_dbt, dim: DagId + Phase):
#   DbtRunDuration   — wall-clock seconds per dbt phase
#   DbtModelsErrored — count of dbt models that errored
#   DbtTestsFailed   — count of dbt tests that failed
#
# All custom metrics live under the Comtrade/Pipeline namespace.  SEARCH
# expressions aggregate across all dimension values automatically.
#
# Athena cost alarm:
#   ProcessedBytes in AWS/Athena namespace — alert when bytes scanned per
#   5-minute window exceeds var.athena_bytes_scanned_alarm_gb.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  dashboard_name = "${local.name_prefix}-comtrade-pipeline"
  cw_namespace   = "Comtrade/Pipeline"
  # Default aggregation window: 1 day.  Override per-widget where needed.
  cw_period_1d = 86400
}

resource "aws_cloudwatch_dashboard" "comtrade_pipeline" {
  dashboard_name = local.dashboard_name

  dashboard_body = jsonencode({
    widgets = [

      # ── Row 1, left: data volume (row count) ──────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Row Count per DAG Run"
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = local.cw_period_1d
          yAxis   = { left = { min = 0 } }
          metrics = [
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Endpoint} MetricName=\"RowCount\"', 'Sum', ${local.cw_period_1d})"
              id         = "rc"
              label      = "Rows [$${PROP('Dim.DagId')} / $${PROP('Dim.Endpoint')}]"
            }]
          ]
        }
      },

      # ── Row 1, right: quality gate — checks passed vs failed ──────────────
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Quality Gate — Checks Passed vs Failed"
          view    = "timeSeries"
          stacked = true
          region  = var.aws_region
          period  = local.cw_period_1d
          yAxis   = { left = { min = 0 } }
          metrics = [
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Endpoint} MetricName=\"ChecksPassed\"', 'Sum', ${local.cw_period_1d})"
              id         = "cp"
              label      = "Passed [$${PROP('Dim.DagId')}]"
              color      = "#2ca02c"
            }],
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Endpoint} MetricName=\"ChecksFailed\"', 'Sum', ${local.cw_period_1d})"
              id         = "cf"
              label      = "Failed [$${PROP('Dim.DagId')}]"
              color      = "#d62728"
            }]
          ]
        }
      },

      # ── Row 2, left: check failure rate (%) ───────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "Check Failure Rate (%)"
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = local.cw_period_1d
          yAxis   = { left = { min = 0, max = 100 } }
          metrics = [
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Endpoint} MetricName=\"ChecksFailed\"', 'Sum', ${local.cw_period_1d})"
              id         = "cf2"
              visible    = false
            }],
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Endpoint} MetricName=\"ChecksPassed\"', 'Sum', ${local.cw_period_1d})"
              id         = "cp2"
              visible    = false
            }],
            [{
              expression = "(cf2 / (cf2 + cp2)) * 100"
              id         = "rate"
              label      = "Failure Rate (%)"
              color      = "#ff7f0e"
            }]
          ]
        }
      },

      # ── Row 2, right: JSON bytes written to S3 ────────────────────────────
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title   = "JSON Bytes Written to S3"
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = local.cw_period_1d
          yAxis   = { left = { min = 0 } }
          metrics = [
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Endpoint} MetricName=\"JsonBytesWritten\"', 'Sum', ${local.cw_period_1d})"
              id         = "bw"
              label      = "Bytes [$${PROP('Dim.DagId')} / $${PROP('Dim.Endpoint')}]"
            }]
          ]
        }
      },

      # ── Row 3, left: dbt run duration per phase ────────────────────────────
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 12
        height = 6
        properties = {
          title   = "dbt Run Duration (seconds)"
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          period  = local.cw_period_1d
          yAxis   = { left = { min = 0 } }
          metrics = [
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Phase} MetricName=\"DbtRunDuration\"', 'Average', ${local.cw_period_1d})"
              id         = "dur"
              label      = "$${PROP('Dim.Phase')}"
            }]
          ]
        }
      },

      # ── Row 3, right: dbt model errors and test failures ──────────────────
      {
        type   = "metric"
        x      = 12
        y      = 12
        width  = 12
        height = 6
        properties = {
          title   = "dbt Errors and Test Failures"
          view    = "timeSeries"
          stacked = true
          region  = var.aws_region
          period  = local.cw_period_1d
          yAxis   = { left = { min = 0 } }
          metrics = [
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Phase} MetricName=\"DbtModelsErrored\"', 'Sum', ${local.cw_period_1d})"
              id         = "me"
              label      = "Models Errored [$${PROP('Dim.Phase')}]"
              color      = "#d62728"
            }],
            [{
              expression = "SEARCH('{${local.cw_namespace},DagId,Phase} MetricName=\"DbtTestsFailed\"', 'Sum', ${local.cw_period_1d})"
              id         = "tf"
              label      = "Tests Failed [$${PROP('Dim.Phase')}]"
              color      = "#ff7f0e"
            }]
          ]
        }
      }

    ]
  })
}

# ── Athena query cost alarm ────────────────────────────────────────────────────
#
# Athena auto-publishes ProcessedBytes to AWS/Athena when the workgroup has
# publish_cloudwatch_metrics_enabled = true.  This alarm fires when the total
# bytes scanned in a 5-minute window exceeds the configured threshold, giving
# ops a heads-up before the per-query 10 GB hard limit is hit.

resource "aws_cloudwatch_metric_alarm" "athena_bytes_scanned" {
  alarm_name          = "${local.name_prefix}-athena-high-bytes-scanned"
  alarm_description   = "Athena scanned more than ${var.athena_bytes_scanned_alarm_gb} GB in a 5-minute window on the comtrade workgroup — investigate runaway queries."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ProcessedBytes"
  namespace           = "AWS/Athena"
  period              = 300
  statistic           = "Sum"
  # Convert GB to bytes: 1 GB = 1 073 741 824 bytes
  threshold          = var.athena_bytes_scanned_alarm_gb * 1073741824
  treat_missing_data = "notBreaching"

  dimensions = {
    WorkGroup = aws_athena_workgroup.comtrade.id
  }

  tags = local.tags
}
