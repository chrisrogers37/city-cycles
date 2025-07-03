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
    select * from {{ source('raw', 'raw_london_modern') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Standardize column names with proper types
        number as ride_id,
        bike_number as bike_id,
        bike_model,
        start_date::timestamp as start_time,
        end_date::timestamp as stop_time,
        start_station as start_station_name,
        start_station_number as start_station_id,
        end_station as end_station_name,
        end_station_number as end_station_id,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (end_date::timestamp - start_date::timestamp)) as duration_seconds,
        -- Date-derived fields
        date_trunc('day', start_date::timestamp) as date,
        extract(month from start_date::timestamp) as month,
        extract(year from start_date::timestamp) as year,
        CASE WHEN extract(isodow from start_date::timestamp) < 6 THEN 'weekday' ELSE 'weekend' END AS day_type,
        extract(isodow from start_date::timestamp) - 1 as day_of_week, -- 0=Monday
        extract(hour from start_date::timestamp) as hour_of_day,
        -- Add metadata
        source_file,
        'london' as location,
        'modern' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 