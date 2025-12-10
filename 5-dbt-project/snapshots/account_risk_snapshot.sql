{% snapshot account_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['risk_flag_text'],
      invalidate_hard_deletes=True
    )
}}

select
    account_id,
    cast(risk_flag as text) as risk_flag_text
from {{ ref('stg_risk_feed') }}

{% endsnapshot %}
