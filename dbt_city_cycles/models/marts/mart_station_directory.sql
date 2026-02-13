{{ config(
    materialized='table'
) }}

with station_stats as (
    select
        location,
        start_station_id as station_id,
        last(start_station_name order by start_time) as station_name,
        median(start_latitude) as latitude,
        median(start_longitude) as longitude,
        count(*) as total_rides,
        min(date) as first_ride_date,
        max(date) as last_ride_date
    from {{ ref('unified_rides') }}
    where start_station_id is not null
    group by 1, 2
)

select
    location,
    station_id,
    station_name,
    latitude,
    longitude,
    total_rides,
    first_ride_date,
    last_ride_date
from station_stats
order by location, station_id
