



-- The `check` strategy in snapshots computes an MD5 hash of the `check_cols`
-- to determine if a row has changed. PostgreSQL's `md5()` function does not
-- support boolean types directly, so we must cast the `risk_flag` to text
-- for the hash comparison to work.

select
    account_id,
    risk_flag::text as risk_flag -- Cast boolean to text for MD5 hashing
from "airflow"."dev_psalmprax_staging"."stg_risk_feed"

