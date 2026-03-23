/*
  stg_preview
  -----------
  Staging model for the `preview` Iceberg source table.

  Responsibilities:
    - Cast all columns to their correct types (the Iceberg schema infers
      everything from JSON, so numeric columns often arrive as strings).
    - Rename API snake_case-ish fields to friendly underscore names.
    - Drop rows that are missing required keys (reporter, period).
    - Expose a clean, typed surface for downstream silver models.

  Materialized as a VIEW so downstream tables always read the latest Iceberg
  snapshot without an extra copy.
*/

with source as (

    select * from {{ source('bronze', 'preview') }}

),

renamed as (

    select
        -- Identifiers
        cast(reportercode  as integer)  as reporter_code,
        cast(reporteriso   as varchar)  as reporter_iso,
        cast(reporterdesc  as varchar)  as reporter_name,

        cast(partnercode   as integer)  as partner_code,
        cast(partneriso    as varchar)  as partner_iso,
        cast(partnerdesc   as varchar)  as partner_name,

        cast(cmdcode       as varchar)  as commodity_code,
        cast(cmddesc       as varchar)  as commodity_name,

        -- Flow direction
        cast(flowcode      as varchar)  as flow_code,
        cast(flowdesc      as varchar)  as flow_name,

        -- Time dimension
        cast(period        as varchar)  as period,
        cast(freqcode      as varchar)  as freq_code,   -- A (annual) | M (monthly)
        cast(typecode      as varchar)  as type_code,   -- C (commodities) | S (services)

        -- Measures
        cast(primaryvalue  as double)   as trade_value_usd

    from source
    where reportercode is not null
      and period       is not null

)

select * from renamed
