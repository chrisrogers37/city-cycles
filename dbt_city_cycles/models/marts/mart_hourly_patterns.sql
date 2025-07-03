{{ config(
    materialized='table'
) }}

select
    location,
    hour_of_day,
    count(*) as ride_count
from {{ ref('unified_rides') }}
group by 1, 2
order by 1, 2 