-- models/staging/stg_payments.sql

with source as (

    select * from {{ source('on_prem_raw_data', 'raw_payments') }}
)

select
    payment_id,
    source_account_id, -- Renaming for consistency
    destination_account_id, -- Renaming for consistency
    payment_reference,
    amount,
    payment_timestamp -- Renaming for consistency
from source