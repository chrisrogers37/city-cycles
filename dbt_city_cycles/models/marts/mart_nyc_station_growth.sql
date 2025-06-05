{{ config(
    materialized='table'
) }}

with station_counts as (
    select 
        extract(year from start_time) as year,
        count(distinct start_station_id) as station_count
    from {{ ref('int_nyc_rides') }}
    group by 1
),
growth_calc as (
    select 
        year,
        station_count,
        lag(station_count) over (order by year) as prev_year_count,
        case 
            when lag(station_count) over (order by year) = 0 then null
            else round(((station_count - lag(station_count) over (order by year))::numeric / 
                  lag(station_count) over (order by year) * 100)::numeric, 1)
        end as yoy_growth
    from station_counts
)
select * from growth_calc
order by year 