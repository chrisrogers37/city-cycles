{{ config(
    materialized='incremental',
    unique_key='ride_id',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_london_legacy') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Standardize column names with proper types
        rental_id as ride_id,
        bike_id,
        start_date::timestamp as start_time,
        end_date::timestamp as stop_time,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (end_date::timestamp - start_date::timestamp)) as duration_seconds,
        -- Add metadata
        source_file,
        'london' as location,
        'legacy' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 