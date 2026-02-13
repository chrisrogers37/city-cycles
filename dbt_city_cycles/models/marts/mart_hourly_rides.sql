{{ config(
    materialized='table'
) }}

select
    location,
    date,
    hour_of_day,
    count(*) as ride_count,
    avg(duration_seconds) as avg_duration_seconds,
    sum(case when user_type = 'member' then 1 else 0 end) as member_rides,
    sum(case when user_type = 'casual' then 1 else 0 end) as casual_rides
from {{ ref('unified_rides') }}
group by 1, 2, 3
order by 1, 2, 3
