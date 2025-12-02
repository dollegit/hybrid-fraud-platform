-- models/staging/stg_payments.sql

with source as (
    -- Reference the raw_payments table from your sources.yml file
    select * from {{ source('on_prem_raw_data', 'raw_payments') }}
),

renamed_and_casted as (
    select
        "payment_id"::varchar as payment_id,
        "source_account_id"::varchar as source_account_id,
        "destination_account_id"::varchar as destination_account_id,
        "amount"::numeric(18, 2) as amount,
        "payment_timestamp"::timestamp as payment_timestamp
    from source
)

select * from renamed_and_casted