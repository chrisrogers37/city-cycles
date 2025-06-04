{{ config(
    materialized='table'
) }}

with daily_metrics as (
    select
        date_trunc('day', start_time) as date,
        count(*) as total_rides,
        count(distinct ride_id) as unique_rides,
        count(distinct bike_id) as unique_bikes,
        count(distinct start_station_id) as unique_start_stations,
        count(distinct end_station_id) as unique_end_stations,
        avg(duration_seconds) as avg_duration_seconds,
        -- Peak hour calculation
        extract(hour from start_time) as hour_of_day,
        count(*) as rides_per_hour
    from {{ ref('int_london_rides') }}
    group by 1, 9
),

monthly_metrics as (
    select
        date_trunc('month', date) as month,
        sum(total_rides) as total_rides,
        sum(unique_rides) as unique_rides,
        max(unique_bikes) as unique_bikes,
        max(unique_start_stations) as unique_start_stations,
        max(unique_end_stations) as unique_end_stations,
        avg(avg_duration_seconds) as avg_duration_seconds,
        -- Calculate YoY growth
        lag(sum(total_rides)) over (order by date_trunc('month', date)) as prev_year_rides,
        round((sum(total_rides) - lag(sum(total_rides)) over (order by date_trunc('month', date)))::float / 
              lag(sum(total_rides)) over (order by date_trunc('month', date)) * 100, 1) as yoy_growth
    from daily_metrics
    group by 1
),

peak_hours as (
    select
        date_trunc('month', date) as month,
        hour_of_day,
        sum(rides_per_hour) as total_rides,
        rank() over (partition by date_trunc('month', date) order by sum(rides_per_hour) desc) as hour_rank
    from daily_metrics
    group by 1, 2
)

select
    m.*,
    p.hour_of_day as peak_hour,
    p.total_rides as peak_hour_rides
from monthly_metrics m
left join peak_hours p
    on m.month = p.month
    and p.hour_rank = 1
order by m.month 