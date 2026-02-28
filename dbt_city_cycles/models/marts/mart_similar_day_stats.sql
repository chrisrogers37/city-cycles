{{ config(
    materialized='table'
) }}

/*
    mart_similar_day_stats — Pre-computed ride statistics grouped by weather
    similarity dimensions. Enables "On days like today..." dashboard queries.

    Granularity: Two levels in a single table, distinguished by `grain`:
      1. 'daily'  — one row per (location, month_num, day_type, temperature_band, precipitation_intensity)
                     Includes peak_hour_start/peak_hour_end derived from the hourly grain.
      2. 'hourly' — one row per (location, month_num, day_type, temperature_band, precipitation_intensity, hour_of_day)

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

-- Overall average rides per day per location (baseline for pct_change_vs_overall)
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

-- Daily totals per date + dimension combination
daily_totals as (
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
),

-- Daily grain: aggregate per (location, month_num, day_type, temperature_band, precipitation_intensity)
daily_stats as (
    select
        d.location,
        d.month_num,
        d.day_type,
        d.temperature_band,
        d.precipitation_intensity,
        count(distinct d.date) as sample_days,
        avg(d.daily_rides) as avg_daily_rides,
        avg(d.daily_avg_duration_seconds) / 60.0 as avg_duration_minutes,
        avg(d.daily_member_rides) as avg_member_rides,
        avg(d.daily_casual_rides) as avg_casual_rides,
        -- Rides pct change vs overall
        case
            when b.overall_avg_daily_rides is null or b.overall_avg_daily_rides = 0 then null
            else round(
                ((avg(d.daily_rides) - b.overall_avg_daily_rides)
                 / b.overall_avg_daily_rides * 100)::float,
                1
            )
        end as pct_change_vs_overall,
        -- Duration pct change vs overall
        case
            when b.overall_avg_duration_seconds is null or b.overall_avg_duration_seconds = 0 then null
            else round(
                ((avg(d.daily_avg_duration_seconds) - b.overall_avg_duration_seconds)
                 / b.overall_avg_duration_seconds * 100)::float,
                1
            )
        end as duration_pct_change_vs_overall
    from daily_totals d
    left join overall_baseline b
        on d.location = b.location
    group by
        d.location,
        d.month_num,
        d.day_type,
        d.temperature_band,
        d.precipitation_intensity,
        b.overall_avg_daily_rides,
        b.overall_avg_duration_seconds
),

-- Hourly grain: aggregate per (location, month_num, day_type, temperature_band, precipitation_intensity, hour_of_day)
hourly_stats as (
    select
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        c.hour_of_day,
        count(distinct c.date) as sample_days,
        avg(c.ride_count) as avg_rides,
        avg(c.avg_duration_seconds) / 60.0 as avg_duration_minutes,
        avg(c.member_rides) as avg_member_rides,
        avg(c.casual_rides) as avg_casual_rides
    from correlation c
    group by
        c.location,
        c.month_num,
        c.day_type,
        c.temperature_band,
        c.precipitation_intensity,
        c.hour_of_day
),

-- Peak hour per dimension combination (hour with max avg rides)
peak_hours as (
    select
        location,
        month_num,
        day_type,
        temperature_band,
        precipitation_intensity,
        hour_of_day as peak_hour_start,
        least(hour_of_day + 2, 23) as peak_hour_end
    from hourly_stats
    qualify row_number() over (
        partition by location, month_num, day_type, temperature_band, precipitation_intensity
        order by avg_rides desc
    ) = 1
)

-- Combine both grains into a single table
select
    'daily' as grain,
    d.location,
    d.month_num,
    d.day_type,
    d.temperature_band,
    d.precipitation_intensity,
    cast(null as integer) as hour_of_day,
    d.sample_days,
    round(d.avg_daily_rides, 1) as avg_daily_rides,
    round(d.avg_duration_minutes, 1) as avg_duration_minutes,
    round(d.avg_member_rides, 1) as avg_member_rides,
    round(d.avg_casual_rides, 1) as avg_casual_rides,
    d.pct_change_vs_overall,
    d.duration_pct_change_vs_overall,
    p.peak_hour_start,
    p.peak_hour_end
from daily_stats d
left join peak_hours p
    on d.location = p.location
    and d.month_num = p.month_num
    and d.day_type = p.day_type
    and d.temperature_band = p.temperature_band
    and d.precipitation_intensity = p.precipitation_intensity

union all

select
    'hourly' as grain,
    h.location,
    h.month_num,
    h.day_type,
    h.temperature_band,
    h.precipitation_intensity,
    h.hour_of_day,
    h.sample_days,
    round(h.avg_rides, 1) as avg_daily_rides,
    round(h.avg_duration_minutes, 1) as avg_duration_minutes,
    round(h.avg_member_rides, 1) as avg_member_rides,
    round(h.avg_casual_rides, 1) as avg_casual_rides,
    cast(null as float) as pct_change_vs_overall,
    cast(null as float) as duration_pct_change_vs_overall,
    cast(null as integer) as peak_hour_start,
    cast(null as integer) as peak_hour_end
from hourly_stats h

order by location, month_num, day_type, temperature_band, precipitation_intensity, grain, hour_of_day
