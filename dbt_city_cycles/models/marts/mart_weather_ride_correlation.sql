{{ config(
    materialized='table'
) }}

select
    r.location,
    r.date,
    r.hour_of_day,
    -- Ride metrics
    r.ride_count,
    r.avg_duration_seconds,
    r.member_rides,
    r.casual_rides,
    -- Weather metrics
    w.temperature_celsius,
    w.apparent_temperature_celsius,
    w.relative_humidity_pct,
    w.precipitation_mm,
    w.rain_mm,
    w.snowfall_cm,
    w.snow_depth_m,
    w.weather_code,
    w.weather_condition,
    w.cloud_cover_pct,
    w.wind_speed_kmh,
    w.wind_gusts_kmh,
    w.is_precipitation,
    w.precipitation_intensity,
    w.temperature_band,
    w.wind_category
from {{ ref('mart_hourly_rides') }} r
inner join {{ ref('stg_weather_hourly') }} w
    on r.location = w.city
    and r.date = w.date
    and r.hour_of_day = w.hour_of_day
