{{ config(
    materialized='table'
) }}

select
    location,
    date_trunc('month', start_time) as month,
    count(*) filter (where user_type = 'member') * 100.0 / count(*) as member_percentage
from {{ ref('int_nyc_rides') }}
group by 1, 2
order by 2 