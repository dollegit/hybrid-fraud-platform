-- models/staging/stg_payments.sql

with source as (

    select * from {{ source('on_prem_raw_data', 'raw_payments') }}
)

select
    payment_id,
    src_account_id as source_account_id, -- Renaming for consistency
    dest_account_id as destination_account_id, -- Renaming for consistency
    payment_reference,
    amount,
    timestamp as payment_timestamp -- Renaming for consistency
from source