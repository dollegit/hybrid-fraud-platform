-- This configuration tells dbt to create this model as a physical table in the data warehouse.
{{ config(materialized='table') }}

-- This dbt model translates the logic from `consolidate_data.py` into SQL.
-- It assumes that staging models (e.g., stg_payments) have already been created
-- to represent the raw source data loaded by Spark/Glue.

with

payments as (
    -- The `ref` function is how dbt builds dependencies. It tells dbt that this model
    -- depends on `stg_payments` and will substitute this with the correct table name.
    select * from {{ ref('stg_payments') }}

    {#
      This block allows for incremental processing by filtering on a date range
      if the 'start_date' and 'end_date' variables are provided from Airflow.
    #}
    {% if var('start_date', false) and var('end_date', false) %}
    where payment_timestamp >= '{{ var("start_date") }}' and payment_timestamp < '{{ var("end_date") }}'
    {% endif %}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

-- Use the SCD Type 2 snapshot of account risk instead of the staging table.
risk_snapshot as (
    select * from {{ ref('account_risk_snapshot') }}
),

fraud_cases as (
    select * from {{ ref('stg_fraud_cases') }}
),

-- Step 1: Join with source and destination account details
joined_accounts as (
    select
        p.*,
        src_acc.opening_date as source_opening_date,
        dest_acc.opening_date as destination_opening_date
    from payments as p
    left join accounts as src_acc
        on p.source_account_id = src_acc.account_id
    left join accounts as dest_acc
        on p.destination_account_id = dest_acc.account_id
),

-- Step 2: Join with risk data for both source and destination accounts
joined_risk as (
    select
        ja.*,
        src_risk.risk_flag as source_risk_flag,
        dest_risk.risk_flag as destination_risk_flag
    from joined_accounts as ja
    -- Join for the source account's risk at the time of the payment
    left join risk_snapshot as src_risk
        on ja.source_account_id = src_risk.account_id
        and ja.payment_timestamp >= src_risk.dbt_valid_from
        and ja.payment_timestamp < coalesce(src_risk.dbt_valid_to, '9999-12-31'::timestamp)

    -- Join for the destination account's risk at the time of the payment
    left join risk_snapshot as dest_risk
        on ja.destination_account_id = dest_risk.account_id
        and ja.payment_timestamp >= dest_risk.dbt_valid_from
        and ja.payment_timestamp < coalesce(dest_risk.dbt_valid_to, '9999-12-31'::timestamp)
),

-- Step 3: Final selection, cleaning, and transformation
select
    jr.payment_id,
    jr.source_account_id,
    jr.destination_account_id,
    jr.amount,
    jr.payment_timestamp as timestamp,
    jr.source_opening_date,
    jr.destination_opening_date,
    coalesce(jr.source_risk_flag, 0) as source_risk_flag, -- Default missing risk flags to 0
    coalesce(jr.destination_risk_flag, 0) as destination_risk_flag, -- Default missing risk flags to 0
    (fc.fraud_type is not null) as is_fraud_flag, -- Create a boolean fraud flag
    fc.fraud_type,
    fc.fraud_reported_date
from joined_risk as jr
left join fraud_cases as fc
    on jr.payment_id = fc.payment_id