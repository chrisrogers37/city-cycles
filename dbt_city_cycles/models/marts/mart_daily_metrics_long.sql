{{ config(
    materialized='table'
) }}

with combined_daily as (
    select date, location, year, total_rides, avg_duration_minutes, population, rides_per_1000
    from {{ ref('mart_nyc_daily_metrics') }}
    union all
    select date, location, year, total_rides, avg_duration_minutes, population, rides_per_1000
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