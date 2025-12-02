-- This singular test checks for data integrity between the final model and a staging model.
-- It ensures that every fraud case from the staging layer is correctly represented
-- in the final `unified_payment_intelligence` model.
-- The test will pass if this query returns zero rows.

with final_model_fraud_count as (
    select count(distinct payment_id) as total_fraud
    from {{ ref('unified_payment_intelligence') }}
    where is_fraud_flag = true
),

staging_fraud_count as (
    select count(distinct payment_id) as total_fraud
    from {{ ref('stg_fraud_cases') }}
)

select
    f.total_fraud as final_model_count,
    s.total_fraud as staging_model_count
from final_model_fraud_count f, staging_fraud_count s
where f.total_fraud != s.total_fraud