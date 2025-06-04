{{ config(
    materialized='table',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']},
        {'columns': ['user_type']}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_nyc_modern') }}
),

renamed as (
    select
        -- Standardize column names with proper types
        ride_id,
        rideable_type,
        started_at::timestamp as start_time,
        ended_at::timestamp as stop_time,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat::double precision as start_latitude,
        start_lng::double precision as start_longitude,
        end_lat::double precision as end_latitude,
        end_lng::double precision as end_longitude,
        member_casual as user_type,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (ended_at::timestamp - started_at::timestamp)) as duration_seconds,
        -- Add metadata
        source_file,
        'nyc' as location,
        'modern' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 