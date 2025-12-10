select
    account_id,
    cast(risk_flag as text) as risk_flag_text
    -- You can also add the updated_at column here if it comes from the source
    -- updated_at
from
    {{ source('on_prem_raw_data', 'raw_risk_feed') }}