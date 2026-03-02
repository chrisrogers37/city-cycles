with source as (
    select * from {{ source('raw', 'raw_nyc_legacy') }}
),

renamed as (
    select
        -- Create unique ride_id using concatenation of key fields
        'legacy_' || bikeid || '_' || 
        start_station_id || '_' || 
        strftime('%Y%m%d%H%M%S', starttime::timestamp) || '_' ||
        strftime('%Y%m%d%H%M%S', stoptime::timestamp) as ride_id,
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
        {{ user_type_mapping('usertype') }} as user_type,
        birth_year::integer AS birth_year,
        gender::integer as gender,
        -- Date-derived fields
        date_trunc('day', starttime::timestamp) as date,
        extract(month from starttime::timestamp) as month,
        extract(year from starttime::timestamp) as year,
        {{ day_type('starttime') }} AS day_type,
        extract(isodow from starttime::timestamp) - 1 as day_of_week, -- 0=Monday
        extract(hour from starttime::timestamp) as hour_of_day,
        -- Add metadata
        source_file,
        'nyc' as location,
        'legacy' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 