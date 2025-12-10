-- models/staging/stg_risk_feed.sql

with renamed_and_casted as (
    select
        "ACCOUNT_ID"::varchar as account_id,
        ("RISK_FLAG"::integer = 1) as risk_flag
    from {{ source('on_prem_raw_data', 'raw_risk_feed') }}
)

select * from renamed_and_casted