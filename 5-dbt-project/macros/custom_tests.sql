{#
  This macro defines a generic test to check if a timestamp column's value
  is always after (or equal to) another date/timestamp column's value.
#}

{% test is_after(model, column_name, other_column_name) %}

select *
from {{ model }}
where {{ column_name }} < {{ other_column_name }}

{% endtest %}

{#
  This macro defines a generic test to check if a model has a minimum number of rows.
  It fails if the row count is less than the specified minimum.
#}
{% test assert_row_count_greater_than(model, min_rows=1) %}

with row_count as (
    select count(*) as total_rows
    from {{ model }}
)
select total_rows
from row_count
where total_rows < {{ min_rows }}

{% endtest %}