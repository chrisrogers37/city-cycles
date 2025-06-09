{{ config(
    materialized='table'
) }}

with station_counts as (
    select 
        location,
        extract(year from start_time) as year,
        count(distinct start_station_id) as station_count
    from {{ ref('unified_rides') }}
    group by 1, 2
),
growth_calc as (
    select 
        s.location,
        s.year,
        s.station_count,
        p.population,
        (s.station_count::float / nullif(p.population, 0)) * 1000 as stations_per_1000,
        lag(s.station_count) over (partition by s.location order by s.year) as prev_year_count,
        case 
            when lag(s.station_count) over (partition by s.location order by s.year) = 0 then null
            else round(((s.station_count - lag(s.station_count) over (partition by s.location order by s.year))::numeric / 
                  lag(s.station_count) over (partition by s.location order by s.year) * 100)::numeric, 1)
        end as yoy_growth
    from station_counts s
    left join {{ ref('population') }} p
      on s.location = p.location
     and s.year = p.year
)
select * from growth_calc
order by location, year 