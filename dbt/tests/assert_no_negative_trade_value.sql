/*
  Custom singular test: assert_no_negative_trade_value
  -----------------------------------------------------
  Fails (returns rows) if any staging record has a negative trade value.
  Negative values indicate bad API data that was not caught by the
  validate_bronze quality gate.
*/

select
    reporter_code,
    partner_code,
    commodity_code,
    flow_code,
    period,
    trade_value_usd
from {{ ref('stg_preview') }}
where trade_value_usd is not null
  and trade_value_usd < 0
