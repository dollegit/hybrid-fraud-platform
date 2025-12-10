{% snapshot account_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['account_id','stg_risk_feed'],
      invalidate_hard_deletes=True
    )
}}

select
    account_id,
    stg_risk_feed
from {{ ref('stg_risk_feed') }}

{% endsnapshot %}
