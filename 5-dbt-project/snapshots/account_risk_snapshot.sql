{% snapshot account_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['account_id','risk_flag'],
      invalidate_hard_deletes=True
    )
}}

select
    account_id,
    risk_flag
from {{ ref('stg_risk_feed') }}

{% endsnapshot %}
