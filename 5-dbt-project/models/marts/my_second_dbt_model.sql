-- Use the `ref` function to select from other models

{{ config(materialized='view') }}

select
    id,
    city,
    '{{ var("my_variable", "default_value") }}' as my_var
from {{ ref('my_first_dbt_model') }}
where id = 1