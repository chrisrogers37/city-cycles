{{ config(
    materialized='table'
) }}

select
    location,
    date_trunc('month', start_time) as month,
    count(*) filter (where user_type = 'member') * 100.0 / count(*) as member_percentage
from {{ ref('unified_rides') }}
where location = 'nyc'
group by 1, 2
order by 2 