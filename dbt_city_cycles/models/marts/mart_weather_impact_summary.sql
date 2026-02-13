{{ config(
    materialized='table'
) }}

with baseline as (
    -- Baseline: average rides per (location, hour_of_day) during clear weather
    -- weather_condition = 'clear' from stg_weather_hourly corresponds to weather_code 0
    select
        location,
        hour_of_day,
        avg(ride_count) as baseline_avg_rides,
        avg(avg_duration_seconds) as baseline_avg_duration_seconds
    from {{ ref('mart_weather_ride_correlation') }}
    where weather_condition = 'clear'
    group by 1, 2
),

by_weather_condition as (
    select
        c.location,
        c.hour_of_day,
        c.weather_condition,
        count(*) as observation_count,
        avg(c.ride_count) as avg_rides,
        avg(c.avg_duration_seconds) as avg_duration_seconds,
        avg(c.member_rides) as avg_member_rides,
        avg(c.casual_rides) as avg_casual_rides,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds,
        case
            when b.baseline_avg_rides is null or b.baseline_avg_rides = 0 then null
            else round(
                ((avg(c.ride_count) - b.baseline_avg_rides)::float / b.baseline_avg_rides * 100)::float,
                1
            )
        end as pct_change_rides_vs_clear,
        case
            when b.baseline_avg_duration_seconds is null or b.baseline_avg_duration_seconds = 0 then null
            else round(
                ((avg(c.avg_duration_seconds) - b.baseline_avg_duration_seconds)::float / b.baseline_avg_duration_seconds * 100)::float,
                1
            )
        end as pct_change_duration_vs_clear
    from {{ ref('mart_weather_ride_correlation') }} c
    left join baseline b
        on c.location = b.location
        and c.hour_of_day = b.hour_of_day
    group by
        c.location,
        c.hour_of_day,
        c.weather_condition,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds
),

by_precipitation_temp as (
    select
        c.location,
        c.hour_of_day,
        c.is_precipitation,
        c.temperature_band,
        count(*) as observation_count,
        avg(c.ride_count) as avg_rides,
        avg(c.avg_duration_seconds) as avg_duration_seconds,
        avg(c.member_rides) as avg_member_rides,
        avg(c.casual_rides) as avg_casual_rides,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds,
        case
            when b.baseline_avg_rides is null or b.baseline_avg_rides = 0 then null
            else round(
                ((avg(c.ride_count) - b.baseline_avg_rides)::float / b.baseline_avg_rides * 100)::float,
                1
            )
        end as pct_change_rides_vs_clear,
        case
            when b.baseline_avg_duration_seconds is null or b.baseline_avg_duration_seconds = 0 then null
            else round(
                ((avg(c.avg_duration_seconds) - b.baseline_avg_duration_seconds)::float / b.baseline_avg_duration_seconds * 100)::float,
                1
            )
        end as pct_change_duration_vs_clear
    from {{ ref('mart_weather_ride_correlation') }} c
    left join baseline b
        on c.location = b.location
        and c.hour_of_day = b.hour_of_day
    group by
        c.location,
        c.hour_of_day,
        c.is_precipitation,
        c.temperature_band,
        b.baseline_avg_rides,
        b.baseline_avg_duration_seconds
)

select
    location,
    hour_of_day,
    'weather_condition' as dimension_type,
    weather_condition as dimension_value,
    cast(null as boolean) as is_precipitation,
    cast(null as varchar) as temperature_band,
    observation_count,
    avg_rides,
    avg_duration_seconds,
    avg_member_rides,
    avg_casual_rides,
    baseline_avg_rides,
    baseline_avg_duration_seconds,
    pct_change_rides_vs_clear,
    pct_change_duration_vs_clear
from by_weather_condition

union all

select
    location,
    hour_of_day,
    'precip_temp' as dimension_type,
    cast(null as varchar) as dimension_value,
    is_precipitation,
    temperature_band,
    observation_count,
    avg_rides,
    avg_duration_seconds,
    avg_member_rides,
    avg_casual_rides,
    baseline_avg_rides,
    baseline_avg_duration_seconds,
    pct_change_rides_vs_clear,
    pct_change_duration_vs_clear
from by_precipitation_temp

order by location, hour_of_day, dimension_type, dimension_value
