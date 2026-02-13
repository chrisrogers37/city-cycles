{{ config(
    materialized='table'
) }}

select
    location,
    hour_of_day,
    sum(ride_count) as ride_count
from {{ ref('mart_hourly_rides') }}
group by 1, 2
order by 1, 2
