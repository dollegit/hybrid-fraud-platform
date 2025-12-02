-- models/staging/stg_fraud_cases.sql

with source as (
    select * from raw_fraud_cases
),

renamed_and_casted as (
    select
        "payment_id"::varchar as payment_id,
        "fraud_type"::varchar as fraud_type,
        "fraud_reported_date"::date as fraud_reported_date
    from source
)

select * from renamed_and_casted