# ── Athena workgroup ──────────────────────────────────────────────────────────
#
# A dedicated workgroup for Comtrade queries.  All results land in a versioned
# S3 prefix; a 10 GB per-query byte-scan limit prevents runaway costs.
# CloudWatch metrics are published so query volume can be tracked on the
# existing dashboard.

resource "aws_athena_workgroup" "comtrade" {
  name        = "${local.name_prefix}-comtrade"
  description = "Athena workgroup for Comtrade trade data queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    # 10 GB safety limit per query — protects against accidental full-table scans.
    bytes_scanned_cutoff_per_query = 10737418240
  }

  tags = local.tags
}


# ── Named queries ─────────────────────────────────────────────────────────────
#
# Tables are in the Glue databases created by dbt-athena:
#   comtrade_silver  — silver Iceberg tables  (trade_flows, reporter_summary)
#   comtrade_staging — staging views           (stg_preview, stg_mbs)
#
# Replace the period/country literals in each query with the values you want.


resource "aws_athena_named_query" "top_exporters_by_period" {
  name      = "top-exporters-by-period"
  workgroup = aws_athena_workgroup.comtrade.id
  database  = "comtrade_silver"

  description = <<-DESC
    Top 20 countries ranked by total export value for a given annual period.
    Edit the WHERE clause to change the target year.
  DESC

  query = <<-SQL
    -- Top 20 exporters for a given year.
    -- Replace '2023' with the period you want to analyse.
    SELECT
        reporter_iso                                    AS country_iso,
        reporter_name                                   AS country_name,
        period,
        ROUND(export_value_usd / 1e9, 2)               AS export_usd_bn,
        ROUND(import_value_usd / 1e9, 2)               AS import_usd_bn,
        ROUND(trade_balance_usd / 1e9, 2)              AS balance_usd_bn,
        commodity_count,
        partner_count
    FROM reporter_summary
    WHERE freq_code = 'A'
      AND period    = '2023'
    ORDER BY export_value_usd DESC
    LIMIT 20;
  SQL
}


resource "aws_athena_named_query" "top_commodities_by_reporter" {
  name      = "top-commodities-by-reporter"
  workgroup = aws_athena_workgroup.comtrade.id
  database  = "comtrade_silver"

  description = <<-DESC
    Top 20 commodities traded by a specific reporting country in a given year.
    Edit the WHERE clause for the reporter ISO code and period.
  DESC

  query = <<-SQL
    -- Top 20 commodities for a reporter country in a given year.
    -- Replace 'USA' and '2023' with your target values.
    SELECT
        commodity_code,
        commodity_name,
        flow_code,
        flow_name,
        ROUND(trade_value_usd / 1e6, 2)    AS trade_value_usd_mn,
        source_record_count
    FROM trade_flows
    WHERE reporter_iso = 'USA'
      AND period       = '2023'
      AND freq_code    = 'A'
    ORDER BY trade_value_usd DESC
    LIMIT 20;
  SQL
}


resource "aws_athena_named_query" "bilateral_trade_balance" {
  name      = "bilateral-trade-balance"
  workgroup = aws_athena_workgroup.comtrade.id
  database  = "comtrade_silver"

  description = <<-DESC
    Imports vs exports between two specific countries across all available
    annual periods.  Shows the trade balance trend over time.
  DESC

  query = <<-SQL
    -- Trade balance between two countries over time.
    -- Replace 'USA' and 'CHN' with your reporter and partner ISO codes.
    SELECT
        period,
        reporter_iso,
        partner_iso,
        partner_name,
        flow_code,
        flow_name,
        ROUND(trade_value_usd / 1e9, 3)    AS trade_value_usd_bn,
        source_record_count
    FROM trade_flows
    WHERE reporter_iso = 'USA'
      AND partner_iso  = 'CHN'
      AND freq_code    = 'A'
      AND commodity_code = 'TOTAL'
    ORDER BY period, flow_code;
  SQL
}


resource "aws_athena_named_query" "yoy_growth_by_reporter" {
  name      = "yoy-growth-by-reporter"
  workgroup = aws_athena_workgroup.comtrade.id
  database  = "comtrade_silver"

  description = <<-DESC
    Year-over-year export and import growth rates for all reporting countries
    between two consecutive years.  Edit the years in the CTEs.
  DESC

  query = <<-SQL
    -- Year-over-year trade growth.
    -- Replace '2022' and '2023' with your base year and comparison year.
    WITH base AS (
        SELECT reporter_iso, reporter_name,
               export_value_usd AS export_base,
               import_value_usd AS import_base
        FROM   reporter_summary
        WHERE  freq_code = 'A' AND period = '2022'
    ),
    current AS (
        SELECT reporter_iso,
               export_value_usd AS export_curr,
               import_value_usd AS import_curr
        FROM   reporter_summary
        WHERE  freq_code = 'A' AND period = '2023'
    )
    SELECT
        b.reporter_iso,
        b.reporter_name,
        ROUND(c.export_curr / 1e9, 2)                                   AS export_2023_usd_bn,
        ROUND(b.export_base / 1e9, 2)                                   AS export_2022_usd_bn,
        ROUND(100.0 * (c.export_curr - b.export_base) / NULLIF(b.export_base, 0), 1)
                                                                         AS export_growth_pct,
        ROUND(100.0 * (c.import_curr - b.import_base) / NULLIF(b.import_base, 0), 1)
                                                                         AS import_growth_pct
    FROM   base b
    JOIN   current c USING (reporter_iso)
    WHERE  b.export_base > 0
    ORDER  BY export_growth_pct DESC
    LIMIT  30;
  SQL
}


resource "aws_athena_named_query" "data_freshness_check" {
  name      = "data-freshness-check"
  workgroup = aws_athena_workgroup.comtrade.id
  database  = "comtrade_silver"

  description = <<-DESC
    Shows the most recent period available for each reporting country.
    Use this to verify that the pipeline has loaded recent data before running
    production analyses.
  DESC

  query = <<-SQL
    -- Latest available period per reporter — use to verify pipeline freshness.
    SELECT
        reporter_iso,
        reporter_name,
        MAX(period)                          AS latest_period,
        COUNT(DISTINCT period)               AS period_count,
        ROUND(MAX(total_trade_value_usd) / 1e9, 2)
                                             AS latest_total_trade_usd_bn
    FROM reporter_summary
    WHERE freq_code = 'A'
    GROUP BY reporter_iso, reporter_name
    ORDER BY latest_period DESC, latest_total_trade_usd_bn DESC
    LIMIT 50;
  SQL
}
