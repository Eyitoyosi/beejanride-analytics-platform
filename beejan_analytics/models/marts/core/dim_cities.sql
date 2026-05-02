{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with cities as (
    select * from {{ ref('stg_cities') }}
),

trip_stats as (
    select
        city_id,
        count(*)                    as total_trips,
        sum(actual_fare)            as total_gross_revenue,
        min(trip_date)              as first_trip_date,
        max(trip_date)              as last_trip_date
    from {{ ref('int_trips_enriched') }}
    where trip_status = 'completed'
    group by city_id
)

select
    c.city_id,
    c.city_name,
    c.country,
    c.launch_date,
    date_diff(current_date(), c.launch_date, month)  as months_since_launch,

    coalesce(ts.total_trips, 0)           as total_trips,
    coalesce(ts.total_gross_revenue, 0)   as total_gross_revenue,
    ts.first_trip_date,
    ts.last_trip_date

from cities c
left join trip_stats ts using (city_id)
