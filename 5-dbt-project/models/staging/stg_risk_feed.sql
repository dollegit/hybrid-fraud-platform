-- models/staging/stg_risk_feed.sql

with source as (

    select * from {{ source('on_prem_raw_data', 'raw_risk_feed') }}

)

select
    account_id,
    risk_score
from source