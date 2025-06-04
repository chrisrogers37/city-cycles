with source as (
    select * from {{ source('raw', 'raw_london_legacy') }}
),

renamed as (
    select
        -- Standardize column names
        rental_id,
        duration as duration_seconds,
        bike_id,
        end_date as stop_time,
        end_station_id,
        end_station_name,
        start_date as start_time,
        start_station_id,
        start_station_name,
        -- Add metadata
        'london' as city,
        'legacy' as schema_version,
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed 