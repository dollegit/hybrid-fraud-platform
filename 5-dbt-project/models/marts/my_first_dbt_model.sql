/*
    Welcome to your first dbt model!
    Did you know that you can use Jinja in your SQL files?
    See an example below:
*/

{{ config(materialized='table') }}

with source_data as (
    select 1 as id, 'new_york' as city
)

select *
from source_data