{{ config(
    materialized='table'
) }}

with station_counts as (
    select
        location,
        extract(year from start_time) as year,
        count(distinct start_station_id) as station_count
    from {{ ref('unified_rides') }}
    where start_station_id is not null
    group by 1, 2
),

station_with_population as (
    select
        s.location,
        s.year,
        s.station_count,
        p.population,
        (s.station_count::float / nullif(p.population, 0)) * 1000 as stations_per_1000,
        lag(s.station_count) over (partition by s.location order by s.year) as prev_year_count
    from station_counts s
    left join {{ ref('population') }} p
      on s.location = p.location
     and s.year = p.year
),

growth_calc as (
    select
        location,
        year,
        station_count,
        population,
        stations_per_1000,
        prev_year_count,
        case
            when prev_year_count is null or prev_year_count = 0 then null
            else round(((station_count - prev_year_count)::float / prev_year_count * 100)::float, 1)
        end as yoy_growth
    from station_with_population
)

select * from growth_calc
order by location, year
