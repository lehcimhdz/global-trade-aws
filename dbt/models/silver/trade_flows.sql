/*
  trade_flows
  -----------
  Silver table: aggregated bilateral trade flows.

  Aggregates the staging trade records to one row per
  (period, reporter, partner, commodity, flow) combination,
  summing trade values when the source contains partial loads.

  Materialized as an Iceberg table partitioned by `period` so that
  downstream consumers and Athena queries can prune efficiently.
*/

with base as (

    select
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
        trade_value_usd

    from {{ ref('stg_preview') }}
    where trade_value_usd is not null
      and trade_value_usd >= 0

),

aggregated as (

    select
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

        sum(trade_value_usd)  as trade_value_usd,
        count(*)              as source_record_count

    from base
    group by
        period, freq_code, type_code,
        reporter_code, reporter_iso, reporter_name,
        partner_code,  partner_iso,  partner_name,
        commodity_code, commodity_name,
        flow_code, flow_name

)

select * from aggregated
