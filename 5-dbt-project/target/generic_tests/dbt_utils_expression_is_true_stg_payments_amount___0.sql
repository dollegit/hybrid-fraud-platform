{{ config({"severity":"Warn"}) }}
{{ dbt_utils.test_expression_is_true(column_name="amount", expression=">= 0", model=get_where_subquery(ref('stg_payments'))) }}