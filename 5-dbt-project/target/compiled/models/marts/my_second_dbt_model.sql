-- Use the `ref` function to select from other models



select
    id,
    city,
    'default_value' as my_var
from "airflow"."dev_psalmprax"."my_first_dbt_model"
where id = 1