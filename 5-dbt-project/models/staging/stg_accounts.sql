-- models/staging/stg_accounts.sql

-- This model cleans and standardizes the raw account data.
-- It selects from the raw_accounts source and can be used for basic transformations.

select
    account_id,
    account_holder_name,
    opening_date as account_opening_date,
    processing_ts
from {{ source('on_prem_raw_data', 'raw_accounts') }}