/*
  stg_mbs
  -------
  Staging model for the `getmbs` (Monthly Bulletin of Statistics) Iceberg table.

  The MBS endpoint returns broad macroeconomic trade indicators rather than
  commodity-level flows.  Column names differ from the preview endpoint.
*/

with source as (

    select * from {{ source('bronze', 'getmbs') }}

),

renamed as (

    select
        -- Time dimension
        cast(period       as varchar)  as period,
        cast(freqcode     as varchar)  as freq_code,

        -- Series metadata
        cast(seriestype   as varchar)  as series_type,
        cast(seriesdesc   as varchar)  as series_name,

        -- Reporter
        cast(reportercode as integer)  as reporter_code,
        cast(reporteriso  as varchar)  as reporter_iso,
        cast(reporterdesc as varchar)  as reporter_name,

        -- Measure
        cast(primaryvalue as double)   as trade_value_usd

    from source
    where period is not null

)

select * from renamed
