-- models/staging/stg_fraud_cases.sql

with source as (

    select * from {{ source('on_prem_raw_data', 'raw_fraud_cases') }}

)

select
    payment_id,
    is_fraud as is_fraud_flag, -- If a record exists in the source, it is a confirmed fraud case.
    fraud_type,
    fraud_reported_date
from source