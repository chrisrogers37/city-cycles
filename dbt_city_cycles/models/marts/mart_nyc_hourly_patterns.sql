{{ config(
    materialized='table'
) }}

select
    location,
    extract(hour from start_time) as hour_of_day,
    count(*) as ride_count
from {{ ref('int_nyc_rides') }}
group by 1, 2
order by 2 