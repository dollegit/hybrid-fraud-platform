



select
    account_id,
    cast(risk_flag as text) as risk_flag_text
from "airflow"."dev_psalmprax_staging"."stg_risk_feed"

