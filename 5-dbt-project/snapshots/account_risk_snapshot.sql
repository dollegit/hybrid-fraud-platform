{% snapshot account_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['risk_score', 'risk_flag', 'risk_flag_text'],
    )
}}

-- Selects the data to be snapshotted
select
    account_id,
    risk_score,
    risk_score > 70 as risk_flag,
    case
        when risk_score > 70 then 'High Risk'
        else 'Low Risk'
    end as risk_flag_text
from {{ ref('stg_risk_feed') }}

{% endsnapshot %}