{{ config(materialized='table') }}

select
    m.location,
    m.date,
    m.year,
    m.day_type,
    count(*) as total_rides,
    avg(m.duration_seconds)/60 as avg_duration_minutes,
    sum(case when m.user_type = 'member' then 1 else 0 end) as member_rides,
    sum(case when m.user_type = 'casual' then 1 else 0 end) as casual_rides,
    sum(m.duration_seconds)/60 as total_minutes_biked,
    p.population,
    (count(*)::float / nullif(p.population, 0)) * 1000 as rides_per_1000
from {{ ref('unified_rides') }} m
left join {{ ref('population') }} p
  on m.location = p.location
 and m.year = p.year
group by 1, 2, 3, 4, 10
order by 2, 1 