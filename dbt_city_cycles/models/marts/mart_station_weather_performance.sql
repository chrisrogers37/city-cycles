{{ config(
    materialized='table'
) }}

-- Step 1: Count rides per station per hour per weather condition
with station_hourly_weather as (
    select
        r.location,
        r.start_station_id as station_id,
        r.hour_of_day,
        w.weather_condition,
        count(*) as ride_count,
        avg(r.duration_seconds) / 60.0 as avg_duration_minutes,
        count(distinct r.date) as days_observed
    from {{ ref('unified_rides') }} r
    inner join {{ ref('stg_weather_hourly') }} w
        on r.location = w.city
        and r.date = w.date
        and r.hour_of_day = w.hour_of_day
    where r.start_station_id is not null
    group by 1, 2, 3, 4
),

-- Step 2: Normalize ride counts by observed days
station_normalized as (
    select
        location,
        station_id,
        hour_of_day,
        weather_condition,
        ride_count,
        ride_count::float / nullif(days_observed, 0) as avg_rides_per_day,
        avg_duration_minutes,
        days_observed
    from station_hourly_weather
),

-- Step 3: Clear-weather baseline per station per hour
clear_weather_baseline as (
    select
        location,
        station_id,
        hour_of_day,
        avg_rides_per_day as clear_avg_rides_per_day
    from station_normalized
    where weather_condition = 'clear'
),

-- Step 4: Percentage change vs clear weather
final as (
    select
        s.location,
        s.station_id,
        s.hour_of_day,
        s.weather_condition,
        s.ride_count as total_rides,
        round(s.avg_rides_per_day, 2) as avg_rides_per_day,
        round(s.avg_duration_minutes, 1) as avg_duration_minutes,
        s.days_observed,
        round(
            case
                when c.clear_avg_rides_per_day is null or c.clear_avg_rides_per_day = 0 then null
                else ((s.avg_rides_per_day - c.clear_avg_rides_per_day) / c.clear_avg_rides_per_day * 100)
            end,
            1
        ) as pct_change_vs_clear
    from station_normalized s
    left join clear_weather_baseline c
        on s.location = c.location
        and s.station_id = c.station_id
        and s.hour_of_day = c.hour_of_day
    where s.ride_count >= 100
)

select * from final
order by location, station_id, hour_of_day, weather_condition
