-- models/staging/stg_risk_feed.sql

with source as (
    select * from raw_risk_feed
),

renamed_and_casted as (
    select
        "account_id"::varchar as account_id,
        ("risk_flag"::integer = 1)::boolean as risk_flag
    from source
)

select * from renamed_and_casted