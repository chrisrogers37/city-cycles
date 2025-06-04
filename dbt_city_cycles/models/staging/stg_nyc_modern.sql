with source as (
    select * from {{ source('raw', 'raw_nyc_modern') }}
),

renamed as (
    select
        -- Standardize column names
        ride_id,
        rideable_type,
        started_at as start_time,
        ended_at as stop_time,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat as start_latitude,
        start_lng as start_longitude,
        end_lat as end_latitude,
        end_lng as end_longitude,
        member_casual as user_type,
        -- Calculate duration in seconds
        extract(epoch from (ended_at - started_at)) as duration_seconds,
        -- Add metadata
        'nyc' as city,
        'modern' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 