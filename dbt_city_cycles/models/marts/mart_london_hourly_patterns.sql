{{ config(
    materialized='table'
) }}

select
    extract(hour from start_time) as hour_of_day,
    count(*) as ride_count
from {{ ref('int_london_rides') }}
group by 1
order by 1 