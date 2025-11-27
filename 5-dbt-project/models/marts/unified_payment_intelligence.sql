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
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

risk_feed as (
    select * from {{ ref('stg_risk_feed') }}
),

fraud_cases as (
    select * from {{ ref('stg_fraud_cases') }}
),

-- 1. Join with source and destination account details
joined_accounts as (
    select
        p.*,
        src_acc.opening_date as src_opening_date,
        dest_acc.opening_date as dest_opening_date
    from payments as p
    left join accounts as src_acc
        on p.src_account_id = src_acc.account_id
    left join accounts as dest_acc
        on p.dest_account_id = dest_acc.account_id
),

-- 2. Join with risk data for both source and destination accounts
joined_risk as (
    select
        ja.*,
        src_risk.risk_flag as src_risk_flag,
        dest_risk.risk_flag as dest_risk_flag
    from joined_accounts as ja
    left join risk_feed as src_risk
        on ja.src_account_id = src_risk.account_id
    left join risk_feed as dest_risk
        on ja.dest_account_id = dest_risk.account_id
),

-- 3. Join with historical fraud cases
final_join as (
    select
        jr.*,
        fc.fraud_type,
        fc.fraud_reported_date
    from joined_risk as jr
    left join fraud_cases as fc
        on jr.payment_id = fc.payment_id
)

-- 4. Final selection, cleaning, and transformation
select
    payment_id,
    src_account_id,
    dest_account_id,
    payment_reference,
    amount,
    timestamp,
    src_opening_date,
    dest_opening_date,
    coalesce(src_risk_flag, 0) as src_risk_flag, -- Default missing risk flags to 0
    coalesce(dest_risk_flag, 0) as dest_risk_flag, -- Default missing risk flags to 0
    (fraud_type is not null) as fraud_flag, -- Create boolean fraud_flag
    fraud_type,
    fraud_reported_date
from final_join