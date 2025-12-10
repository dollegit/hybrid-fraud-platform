{% snapshot account_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='check',
      check_cols=['risk_flag'],
      invalidate_hard_deletes=True,
    )
}}

-- This selects the data that we want to track for historical changes.
-- When you run `dbt snapshot`, dbt will compare the current state of this
-- query's result with the existing records in the snapshot table.
-- If the `risk_flag` for an `account_id` has changed, it will expire the old
-- record (setting `dbt_valid_to`) and insert a new one.

select * from {{ ref('stg_risk_feed') }}

{% endsnapshot %}