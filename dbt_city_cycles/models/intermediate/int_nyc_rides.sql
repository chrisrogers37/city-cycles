with modern_rides as (
    select
        ride_id,
        NULL as bike_id,  -- Modern data doesn't have bike_id
        start_time,
        stop_time,
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,
        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,
        user_type,
        duration_seconds,
        date,
        month,
        year,
        day_type,
        day_of_week,
        hour_of_day,
        NULL as birth_year,
        NULL as gender,
        source_file,
        location,
        schema_version,
        dbt_updated_at
    from {{ ref('stg_nyc_modern') }}
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
        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,
        user_type,
        duration_seconds,
        date,
        month,
        year,
        day_type,
        day_of_week,
        hour_of_day,
        birth_year,
        gender,
        source_file,
        location,
        schema_version,
        dbt_updated_at
    from {{ ref('stg_nyc_legacy') }}
),

combined_rides as (
    select * from modern_rides
    union all
    select * from legacy_rides
)

select * from combined_rides 