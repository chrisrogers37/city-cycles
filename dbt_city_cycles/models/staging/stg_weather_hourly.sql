{{ config(
    materialized='incremental',
    unique_key='weather_record_id',
    indexes=[
        {'columns': ['timestamp']},
        {'columns': ['city']},
        {'columns': ['date']},
        {'columns': ['weather_record_id'], 'unique': true}
    ]
) }}

with source as (
    select * from {{ source('raw', 'raw_weather_hourly') }}
    {% if is_incremental() %}
    where source_file not in (select distinct source_file from {{ this }})
    {% endif %}
),

cleaned as (
    select
        -- Generate a unique record ID from city + timestamp
        city || '_' || strftime(timestamp::timestamp, '%Y%m%d%H') as weather_record_id,

        -- Core fields
        timestamp::timestamp as timestamp,
        city,

        -- Temperature fields (Celsius)
        temperature_2m::double precision as temperature_celsius,
        apparent_temperature::double precision as apparent_temperature_celsius,

        -- Humidity
        relative_humidity_2m::double precision as relative_humidity_pct,

        -- Precipitation (mm)
        precipitation::double precision as precipitation_mm,
        rain::double precision as rain_mm,
        snowfall::double precision as snowfall_cm,
        snow_depth::double precision as snow_depth_m,

        -- WMO weather code
        weather_code::integer as weather_code,

        -- Cloud & wind
        cloud_cover::double precision as cloud_cover_pct,
        wind_speed_10m::double precision as wind_speed_kmh,
        wind_gusts_10m::double precision as wind_gusts_kmh,

        -- Derived: human-readable weather condition from WMO code
        CASE
            WHEN weather_code IN (0) THEN 'clear'
            WHEN weather_code IN (1, 2, 3) THEN 'partly_cloudy'
            WHEN weather_code IN (45, 48) THEN 'fog'
            WHEN weather_code IN (51, 53, 55) THEN 'drizzle'
            WHEN weather_code IN (56, 57) THEN 'freezing_drizzle'
            WHEN weather_code IN (61, 63, 65) THEN 'rain'
            WHEN weather_code IN (66, 67) THEN 'freezing_rain'
            WHEN weather_code IN (71, 73, 75) THEN 'snow'
            WHEN weather_code IN (77) THEN 'snow_grains'
            WHEN weather_code IN (80, 81, 82) THEN 'rain_showers'
            WHEN weather_code IN (85, 86) THEN 'snow_showers'
            WHEN weather_code IN (95) THEN 'thunderstorm'
            WHEN weather_code IN (96, 99) THEN 'thunderstorm_hail'
            ELSE 'unknown'
        END as weather_condition,

        -- Derived: is_precipitation flag
        CASE
            WHEN precipitation > 0 OR rain > 0 OR snowfall > 0 THEN true
            ELSE false
        END as is_precipitation,

        -- Derived: precipitation intensity category
        CASE
            WHEN precipitation = 0 THEN 'none'
            WHEN precipitation < 2.5 THEN 'light'
            WHEN precipitation < 7.5 THEN 'moderate'
            WHEN precipitation < 50 THEN 'heavy'
            ELSE 'extreme'
        END as precipitation_intensity,

        -- Derived: temperature band
        CASE
            WHEN temperature_2m < 0 THEN 'freezing'
            WHEN temperature_2m < 10 THEN 'cold'
            WHEN temperature_2m < 20 THEN 'mild'
            WHEN temperature_2m < 30 THEN 'warm'
            ELSE 'hot'
        END as temperature_band,

        -- Derived: wind category (Beaufort-inspired, km/h)
        CASE
            WHEN wind_speed_10m < 12 THEN 'calm'
            WHEN wind_speed_10m < 30 THEN 'light'
            WHEN wind_speed_10m < 50 THEN 'moderate'
            WHEN wind_speed_10m < 75 THEN 'strong'
            ELSE 'severe'
        END as wind_category,

        -- Date-derived fields (matching bike staging model patterns)
        date_trunc('day', timestamp::timestamp) as date,
        date_trunc('hour', timestamp::timestamp) as hour,
        extract(month from timestamp::timestamp) as month,
        extract(year from timestamp::timestamp) as year,
        {{ day_type('timestamp') }} AS day_type,
        extract(isodow from timestamp::timestamp) - 1 as day_of_week,
        extract(hour from timestamp::timestamp) as hour_of_day,

        -- Metadata
        source_file,
        current_timestamp as dbt_updated_at

    from source
    where timestamp is not null
)

select * from cleaned
