-- models/staging/stg_accounts.sql

with source as (

    select * from {{ source('on_prem_raw_data', 'raw_accounts') }}

)

select
    account_id,
    opening_date
from source