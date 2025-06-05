{{ config(
    materialized='table'
) }}

select
    m.location,
    date_trunc('day', m.start_time) as date,
    extract(year from m.start_time) as year,
    count(*) as total_rides,
    avg(m.duration_seconds)/60 as avg_duration_minutes,
    p.population,
    (count(*)::float / nullif(p.population, 0)) * 1000 as rides_per_1000
from {{ ref('int_nyc_rides') }} m
left join {{ ref('population') }} p
  on m.location = p.location
 and extract(year from m.start_time) = p.year
group by 1, 2, 3, 6
order by 2 