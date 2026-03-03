-- NYC data (has latitude/longitude, user_type, birth_year, gender)
SELECT
    ride_id,
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
    bike_id,           -- null for NYC modern
    duration_seconds,
    birth_year,        -- null for NYC modern
    gender,            -- null for NYC modern
    -- Date-derived fields (from staging)
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
FROM {{ ref('int_nyc_rides') }}

UNION ALL

-- London data (no latitude/longitude, user_type, birth_year, gender)
SELECT
    ride_id,
    start_time,
    stop_time,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    NULL as start_latitude,
    NULL as start_longitude,
    NULL as end_latitude,
    NULL as end_longitude,
    NULL as user_type,
    bike_id,
    duration_seconds,
    NULL as birth_year,
    NULL as gender,
    -- Date-derived fields (from staging)
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
FROM {{ ref('int_london_rides') }}