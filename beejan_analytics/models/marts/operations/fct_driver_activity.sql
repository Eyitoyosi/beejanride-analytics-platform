{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "activity_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['driver_id', 'city_id'],
        tags=['marts', 'operations', 'incremental']
    )
}}

/*
  fct_driver_activity
  ────────────────────
  Daily driver activity summary: trips completed, revenue earned,
  and online hours. Used for the Driver Leaderboard and Churn Tracking.
*/

with trips as (
    select * from {{ ref('fct_trips') }}
    where trip_status = 'completed'

    {% if is_incremental() %}
        and trip_date >= date_sub(
            (select max(activity_date) from {{ this }}),
            interval 1 day
        )
    {% endif %}
),

-- Calculate online time per driver per day from status events
events as (
    select * from {{ ref('stg_driver_status_events') }}

    {% if is_incremental() %}
        where event_date >= date_sub(
            (select max(activity_date) from {{ this }}),
            interval 1 day
        )
    {% endif %}
),

-- Pair online events with the next offline event (session windows)
online_sessions as (
    select
        driver_id,
        event_date,
        event_timestamp                       as went_online_at,
        lead(event_timestamp) over (
            partition by driver_id
            order by event_timestamp
        )                                     as went_offline_at,
        driver_online_status
    from events
),

daily_online_hours as (
    select
        driver_id,
        event_date                            as activity_date,
        round(
            sum(
                timestamp_diff(
                    coalesce(went_offline_at, timestamp_add(went_online_at, interval 8 hour)),
                    went_online_at,
                    minute
                )
            ) / 60.0,
            2
        )                                     as online_hours
    from online_sessions
    where driver_online_status = 'online'
    group by driver_id, event_date
),

daily_trips as (
    select
        driver_id,
        city_id,
        trip_date                             as activity_date,
        count(*)                              as trips_completed,
        sum(gross_revenue)                    as gross_revenue,
        sum(net_revenue)                      as net_revenue,
        avg(trip_duration_minutes)            as avg_trip_duration_minutes,
        countif(is_corporate)                 as corporate_trips,
        countif(surge_multiplier > 1.0)       as surge_trips
    from trips
    group by driver_id, city_id, trip_date
)

select
    {{ generate_surrogate_key_from_cols(['dt.driver_id', 'dt.activity_date']) }}
        as fct_driver_activity_sk,

    dt.driver_id,
    dt.city_id,
    dt.activity_date,

    dt.trips_completed,
    dt.gross_revenue,
    dt.net_revenue,
    dt.avg_trip_duration_minutes,
    dt.corporate_trips,
    dt.surge_trips,

    coalesce(oh.online_hours, 0)              as online_hours,
    {{ safe_divide('dt.gross_revenue', 'oh.online_hours') }}
        as revenue_per_online_hour

from daily_trips dt
left join daily_online_hours oh
    on dt.driver_id = oh.driver_id 
    and dt.activity_date = oh.activity_date
