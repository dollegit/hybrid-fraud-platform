-- models/staging/stg_accounts.sql

with source as (
    select * from raw_accounts
),

renamed_and_casted as (
    select
        "account_id"::varchar as account_id,
        "opening_date"::date as opening_date
    from source
)

select * from renamed_and_casted