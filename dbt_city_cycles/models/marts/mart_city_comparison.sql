{{ config(
    materialized='table'
) }}

with nyc_metrics as (
    select
        month,
        total_rides as nyc_total_rides,
        unique_bikes as nyc_unique_bikes,
        unique_start_stations as nyc_unique_stations,
        avg_duration_seconds as nyc_avg_duration,
        member_percentage as nyc_member_percentage,
        yoy_growth as nyc_yoy_growth,
        peak_hour as nyc_peak_hour,
        peak_hour_rides as nyc_peak_hour_rides
    from {{ ref('mart_nyc_metrics') }}
),

london_metrics as (
    select
        month,
        total_rides as london_total_rides,
        unique_bikes as london_unique_bikes,
        unique_start_stations as london_unique_stations,
        avg_duration_seconds as london_avg_duration,
        yoy_growth as london_yoy_growth,
        peak_hour as london_peak_hour,
        peak_hour_rides as london_peak_hour_rides
    from {{ ref('mart_london_metrics') }}
)

select
    n.month,
    -- Raw metrics
    n.nyc_total_rides,
    l.london_total_rides,
    n.nyc_unique_bikes,
    l.london_unique_bikes,
    n.nyc_unique_stations,
    l.london_unique_stations,
    n.nyc_avg_duration,
    l.london_avg_duration,
    n.nyc_member_percentage,
    -- Normalized metrics (per bike)
    round(n.nyc_total_rides::float / n.nyc_unique_bikes, 1) as nyc_rides_per_bike,
    round(l.london_total_rides::float / l.london_unique_bikes, 1) as london_rides_per_bike,
    -- Normalized metrics (per station)
    round(n.nyc_total_rides::float / n.nyc_unique_stations, 1) as nyc_rides_per_station,
    round(l.london_total_rides::float / l.london_unique_stations, 1) as london_rides_per_station,
    -- Growth comparison
    n.nyc_yoy_growth,
    l.london_yoy_growth,
    -- Peak hour comparison
    n.nyc_peak_hour,
    l.london_peak_hour,
    n.nyc_peak_hour_rides,
    l.london_peak_hour_rides
from nyc_metrics n
join london_metrics l
    on n.month = l.month
order by n.month 