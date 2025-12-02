{{ config({"severity":"Warn"}) }}
{{ test_accepted_values(column_name="is_fraud_flag", model=get_where_subquery(ref('unified_payment_intelligence')), values=[true,false]) }}