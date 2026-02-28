{{ config(
    materialized='table'
) }}

/*
    mart_similar_day_stats — Pre-computed ride statistics grouped by weather
    similarity dimensions. Enables "On days like today..." dashboard queries.

    Granularity: Two levels in a single table, distinguished by `grain`:
      1. 'daily'  — one row per (location, month, day_type, temperature_band, precipitation_intensity)
      2. 'hourly' — one row per (location, month, day_type, temperature_band, precipitation_intensity, hour_of_day)

    The dashboard queries this mart by matching today's live weather conditions
    to the appropriate dimension values and reading back the pre-aggregated stats.
*/

with correlation as (
    select
        location,
        date,
        hour_of_day,
        ride_count,
        avg_duration_seconds,
        member_rides,
        casual_rides,
        temperature_band,
        precipitation_intensity,
        -- Extract month number (1-12) from date for seasonal matching
        extract(month from date) as month_num,
        -- Derive day_type from day-of-week (matching existing staging pattern)
        case
            when extract(isodow from date) in (6, 7) then 'weekend'
            else 'weekday'
        end as day_type
    from {{ ref('mart_weather_ride_correlation') }}
),

-- Overall average rides per day per location (baseline for pct_vs_overall)
overall_baseline as (
    select
        location,
        avg(daily_rides) as overall_avg_daily_rides,
        avg(daily_avg_duration_seconds) as overall_avg_duration_seconds
    from (
        select
            location,
            date,
            sum(ride_count) as daily_rides,
            avg(avg_duration_seconds) as daily_avg_duration_seconds
        from correlation
        group by location, date
    ) daily
    group by location
),

-- Daily grain: aggregate per (location, month, day_type, temperature_band, precipitation_intensity)
daily_stats as (
    select
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        -- Number of distinct days matching this combination
        count(distinct c.date) as total_days_observed,
        -- Average rides per day: sum all hourly rides per day, then average across days
        avg(daily_totals.daily_rides) as avg_rides_per_day,
        -- Average duration across all rides in matching days
        avg(daily_totals.daily_avg_duration_seconds) as avg_duration_seconds,
        -- Member/casual split (average per day)
        avg(daily_totals.daily_member_rides) as avg_member_rides_per_day,
        avg(daily_totals.daily_casual_rides) as avg_casual_rides_per_day,
        -- Comparison to overall average
        case
            when b.overall_avg_daily_rides is null or b.overall_avg_daily_rides = 0 then null
            else round(
                ((avg(daily_totals.daily_rides) - b.overall_avg_daily_rides)
                 / b.overall_avg_daily_rides * 100)::float,
                1
            )
        end as pct_vs_overall
    from correlation c
    inner join (
        select
            location,
            date,
            month_num,
            day_type,
            temperature_band,
            precipitation_intensity,
            sum(ride_count) as daily_rides,
            avg(avg_duration_seconds) as daily_avg_duration_seconds,
            sum(member_rides) as daily_member_rides,
            sum(casual_rides) as daily_casual_rides
        from correlation
        group by location, date, month_num, day_type, temperature_band, precipitation_intensity
    ) daily_totals
        on c.location = daily_totals.location
        and c.date = daily_totals.date
        and c.month_num = daily_totals.month_num
        and c.day_type = daily_totals.day_type
        and c.temperature_band = daily_totals.temperature_band
        and c.precipitation_intensity = daily_totals.precipitation_intensity
    left join overall_baseline b
        on c.location = b.location
    group by
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        b.overall_avg_daily_rides,
        b.overall_avg_duration_seconds
),

-- Hourly grain: aggregate per (location, month, day_type, temperature_band, precipitation_intensity, hour_of_day)
hourly_stats as (
    select
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        c.hour_of_day,
        count(distinct c.date) as total_days_observed,
        avg(c.ride_count) as avg_rides_per_hour,
        avg(c.avg_duration_seconds) as avg_duration_seconds,
        avg(c.member_rides) as avg_member_rides_per_hour,
        avg(c.casual_rides) as avg_casual_rides_per_hour
    from correlation c
    group by
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        c.hour_of_day
)

-- Combine both grains into a single table
select
    'daily' as grain,
    location,
    month_num,
    day_type,
    temperature_band,
    precipitation_intensity,
    cast(null as integer) as hour_of_day,
    total_days_observed,
    round(avg_rides_per_day, 1) as avg_rides,
    round(avg_duration_seconds, 1) as avg_duration_seconds,
    round(avg_member_rides_per_day, 1) as avg_member_rides,
    round(avg_casual_rides_per_day, 1) as avg_casual_rides,
    pct_vs_overall
from daily_stats

union all

select
    'hourly' as grain,
    location,
    month_num,
    day_type,
    temperature_band,
    precipitation_intensity,
    hour_of_day,
    total_days_observed,
    round(avg_rides_per_hour, 1) as avg_rides,
    round(avg_duration_seconds, 1) as avg_duration_seconds,
    round(avg_member_rides_per_hour, 1) as avg_member_rides,
    round(avg_casual_rides_per_hour, 1) as avg_casual_rides,
    cast(null as float) as pct_vs_overall
from hourly_stats

order by location, month_num, day_type, temperature_band, precipitation_intensity, grain, hour_of_day
