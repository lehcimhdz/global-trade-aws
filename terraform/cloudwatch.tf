# ── CloudWatch dashboard — Comtrade Pipeline ──────────────────────────────────
#
# Ops-facing view of the four custom metrics emitted by validate_bronze:
#
#   RowCount         — data volume per DAG run
#   ChecksPassed     — quality gate pass count
#   ChecksFailed     — quality gate fail count  (also drives failure-rate widget)
#   JsonBytesWritten — raw S3 data volume
#
# All metrics live under the Comtrade/Pipeline namespace with dimensions
# DagId and Endpoint.  SEARCH expressions aggregate across all dimension
# values automatically, so new DAGs / endpoints appear without any
# dashboard change.
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
      }

    ]
  })
}
