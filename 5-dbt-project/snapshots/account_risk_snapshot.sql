{% snapshot account_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['risk_flag'],
      invalidate_hard_deletes=True
    )
}}

-- The `check` strategy in snapshots computes an MD5 hash of the `check_cols`
-- to determine if a row has changed. PostgreSQL's `md5()` function does not
-- support boolean types directly, so we must cast the `risk_flag` to text
-- for the hash comparison to work.

select
    account_id,
    risk_flag::text as risk_flag -- Cast boolean to text for MD5 hashing
from {{ ref('stg_risk_feed') }}

{% endsnapshot %}