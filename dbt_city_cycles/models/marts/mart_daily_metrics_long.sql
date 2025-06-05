{{ config(
    materialized='table'
) }}

with combined_daily as (
    select date, location, year, total_rides, avg_duration_minutes, member_rides, casual_rides, total_minutes_biked, population, rides_per_1000
    from {{ ref('mart_nyc_daily_metrics') }}
    union all
    select date, location, year, total_rides, avg_duration_minutes, null as member_rides, null as casual_rides, total_minutes_biked, population, rides_per_1000
    from {{ ref('mart_london_daily_metrics') }}
)

select
    date,
    location,
    year,
    'total_rides' as metric_name,
    total_rides::float as metric_value
from combined_daily
union all
select
    date,
    location,
    year,
    'avg_duration_minutes' as metric_name,
    avg_duration_minutes as metric_value
from combined_daily
union all
select
    date,
    location,
    year,
    'member_rides' as metric_name,
    member_rides::float as metric_value
from combined_daily where member_rides is not null
union all
select
    date,
    location,
    year,
    'casual_rides' as metric_name,
    casual_rides::float as metric_value
from combined_daily where casual_rides is not null
union all
select
    date,
    location,
    year,
    'total_minutes_biked' as metric_name,
    total_minutes_biked as metric_value
from combined_daily
union all
select
    date,
    location,
    year,
    'population' as metric_name,
    population::float as metric_value
from combined_daily
union all
select
    date,
    location,
    year,
    'rides_per_1000' as metric_name,
    rides_per_1000 as metric_value
from combined_daily
order by date, location, metric_name 