{{ config(
    materialized='incremental',
    unique_key='ride_id',
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']},
        {'columns': ['user_type']}
    ]
) }}

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
        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,
        user_type,
        duration_seconds,
        source_file,
        location,
        schema_version,
        dbt_updated_at
    from {{ ref('stg_nyc_modern') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
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
        source_file,
        location,
        schema_version,
        dbt_updated_at
    from {{ ref('stg_nyc_legacy') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

combined_rides as (
    select * from modern_rides
    union all
    select * from legacy_rides
)

select * from combined_rides 