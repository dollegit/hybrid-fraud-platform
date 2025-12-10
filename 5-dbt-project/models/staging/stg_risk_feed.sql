-- models/staging/stg_risk_feed.sql

with renamed_and_casted as (
    select
        "account_id"::varchar as account_id,
        ("risk_flag"::integer = 1) as risk_flag
    from {{ source('on_prem_raw_data', 'raw_risk_feed') }}
)

select * from renamed_and_casted