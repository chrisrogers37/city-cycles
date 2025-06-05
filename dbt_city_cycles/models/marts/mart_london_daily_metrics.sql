{{ config(
    materialized='table'
) }}

select
    date_trunc('day', start_time) as date,
    extract(year from start_time) as year,
    count(*) as total_rides,
    avg(duration_seconds)/60 as avg_duration_minutes
from {{ ref('int_london_rides') }}
group by 1, 2
order by 1 