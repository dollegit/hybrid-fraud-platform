{% snapshot account_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['account_id','risk_flag_text'],
    )
}}

-- Selects the data to be snapshotted
select * from {{ ref('stg_risk_feed') }}

{% endsnapshot %}