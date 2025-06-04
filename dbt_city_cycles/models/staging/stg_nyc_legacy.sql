{{ config(
    materialized='incremental',
    unique_key=['bikeid', 'starttime', 'stoptime', 'start_station_id'],
    indexes=[
        {'columns': ['start_time']},
        {'columns': ['ride_id'], 'unique': true},
        {'columns': ['bike_id']},
        {'columns': ['user_type']}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_nyc_legacy') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Create unique ride_id using concatenation of key fields
        'legacy_' || bikeid || '_' || 
        start_station_id || '_' || 
        to_char(starttime::timestamp, 'YYYYMMDDHH24MISS') || '_' ||
        to_char(stoptime::timestamp, 'YYYYMMDDHH24MISS') as ride_id,
        -- Calculate duration in seconds from timestamps
        extract(epoch from (stoptime::timestamp - starttime::timestamp)) as duration_seconds,
        starttime::timestamp as start_time,
        stoptime::timestamp as stop_time,
        start_station_id,
        start_station_name,
        start_station_latitude::double precision as start_latitude,
        start_station_longitude::double precision as start_longitude,
        end_station_id,
        end_station_name,
        end_station_latitude::double precision as end_latitude,
        end_station_longitude::double precision as end_longitude,
        bikeid as bike_id,
        -- Map legacy user types to modern nomenclature
        case 
            when usertype = 'Subscriber' then 'member'
            when usertype = 'Customer' then 'casual'
            else usertype
        end as user_type,
        birth_year::integer,
        gender::integer,
        -- Add metadata
        source_file,
        'nyc' as location,
        'legacy' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 