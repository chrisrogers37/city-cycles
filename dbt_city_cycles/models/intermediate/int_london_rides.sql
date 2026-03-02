with modern_rides as (
    select
        ride_id,
        bike_id,
        start_time,
        stop_time,
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,
        duration_seconds,
        date,
        month,
        year,
        day_type,
        day_of_week,
        hour_of_day,
        source_file,
        location,
        schema_version,
        dbt_updated_at
    from {{ ref('stg_london_modern') }}
),

legacy_rides as (
    select
        ride_id,
        bike_id,
        start_time,
        stop_time,
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,
        duration_seconds,
        date,
        month,
        year,
        day_type,
        day_of_week,
        hour_of_day,
        source_file,
        location,
        schema_version,
        dbt_updated_at
    from {{ ref('stg_london_legacy') }}
),

combined_rides as (
    select * from modern_rides
    union all
    select * from legacy_rides
)

select * from combined_rides 