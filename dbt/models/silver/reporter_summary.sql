/*
  reporter_summary
  ----------------
  Silver table: per-reporter trade summary by period.

  Rolls up all commodity-level flows to country-level totals, splitting
  imports and exports into separate columns.  Useful for dashboards and
  high-level country comparisons without scanning the full trade_flows table.

  Partitioned by `period` for efficient time-range queries.
*/

with flows as (

    select
        period,
        freq_code,
        reporter_code,
        reporter_iso,
        reporter_name,
        partner_code,
        commodity_code,
        flow_code,
        trade_value_usd

    from {{ ref('stg_preview') }}
    where trade_value_usd is not null
      and trade_value_usd >= 0

),

aggregated as (

    select
        period,
        freq_code,
        reporter_code,
        reporter_iso,
        reporter_name,

        sum(
            case when flow_code = 'X' then trade_value_usd else 0 end
        )                                           as export_value_usd,

        sum(
            case when flow_code = 'M' then trade_value_usd else 0 end
        )                                           as import_value_usd,

        sum(trade_value_usd)                        as total_trade_value_usd,

        sum(
            case when flow_code = 'X' then trade_value_usd else 0 end
        ) - sum(
            case when flow_code = 'M' then trade_value_usd else 0 end
        )                                           as trade_balance_usd,

        count(distinct commodity_code)              as commodity_count,
        count(distinct partner_code)                as partner_count

    from flows
    group by
        period, freq_code,
        reporter_code, reporter_iso, reporter_name

)

select * from aggregated
