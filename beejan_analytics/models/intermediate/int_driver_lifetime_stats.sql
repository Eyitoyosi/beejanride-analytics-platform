{{
    config(
        materialized='view',
        tags=['intermediate', 'drivers', 'operations']
    )
}}

/*
  int_driver_lifetime_stats
  ─────────────────────────
  Aggregates lifetime trip, revenue, and activity metrics per driver.
  Used by driver leaderboard and churn monitoring marts.

  View materialization is used here since this aggregates across the entire trip and event history per driver,
  which is expensive to compute but queried frequently by downstream models.
*/

with trips as (
    select * from {{ ref('int_trips_enriched') }}
    where trip_status = 'completed'
),

driver_events as (
    select * from {{ ref('stg_driver_status_events') }}
),

-- Lifetime trip & revenue aggregates
trip_stats as (
    select
        driver_id,
        count(*)                          as lifetime_trips,
        sum(actual_fare)                  as lifetime_gross_revenue,
        sum(net_revenue)                  as lifetime_net_revenue,
        avg(actual_fare)                  as avg_fare_per_trip,
        avg(trip_duration_minutes)        as avg_trip_duration_minutes,
        avg(driver_rating)                as avg_driver_rating,
        min(trip_date)                    as first_trip_date,
        max(trip_date)                    as last_trip_date,
        countif(is_corporate)             as corporate_trips,
        countif(not is_corporate)         as personal_trips,
        countif(surge_multiplier > 1.0)   as surge_trips
    from trips
    group by driver_id
),

-- Last seen online (for churn detection)
last_online as (
    select
        driver_id,
        max(event_timestamp) as last_online_at
    from driver_events
    where driver_online_status = 'online'
    group by driver_id
),

combined as (
    select
        ts.driver_id,
        ts.lifetime_trips,
        ts.lifetime_gross_revenue,
        ts.lifetime_net_revenue,
        ts.avg_fare_per_trip,
        ts.avg_trip_duration_minutes,
        ts.avg_driver_rating,
        ts.first_trip_date,
        ts.last_trip_date,
        ts.corporate_trips,
        ts.personal_trips,
        ts.surge_trips,
        lo.last_online_at,

        -- Churn flag: no trip in last N days
        date_diff(current_date(), ts.last_trip_date, day)
            as days_since_last_trip,

        case
            when date_diff(current_date(), ts.last_trip_date, day)
                 >= {{ var('churn_inactivity_days') }}
            then true
            else false
        end as is_churned

    from trip_stats ts
    left join last_online lo using (driver_id)
)

select * from combined
