-- models/staging/stg_payments.sql

with source as (
    -- In a real project, this would be `{{ source('raw_data', 'payments') }}`
    -- after defining sources. For now, we assume a 'raw_payments' table exists.
    select * from raw_payments
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