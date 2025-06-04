with source as (
    select * from {{ source('raw', 'raw_nyc_legacy') }}
),

renamed as (
    select
        -- Standardize column names
        tripduration as duration_seconds,
        starttime as start_time,
        stoptime as stop_time,
        start_station_id,
        start_station_name,
        start_station_latitude as start_latitude,
        start_station_longitude as start_longitude,
        end_station_id,
        end_station_name,
        end_station_latitude as end_latitude,
        end_station_longitude as end_longitude,
        bikeid as bike_id,
        usertype as user_type,
        birth_year,
        gender,
        -- Add metadata
        'nyc' as city,
        'legacy' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 