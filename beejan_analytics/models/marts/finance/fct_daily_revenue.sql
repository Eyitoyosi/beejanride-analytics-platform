{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "trip_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['city_id'],
        tags=['marts', 'finance', 'incremental']
    )
}}

/*
  fct_daily_revenue
  ─────────────────
  Pre-aggregated daily revenue rollup per city.
  Powers the daily revenue dashboard and city profitability reports.

  Includes gross revenue, net revenue, corporate vs personal split,
  and surge impact metrics.
*/

with trips as (
    select * from {{ ref('fct_trips') }}
    where trip_status = 'completed'

    {% if is_incremental() %}
        and trip_date >= date_sub(
            (select max(trip_date) from {{ this }}),
            interval 1 day
        )
    {% endif %}
),

cities as (
    select city_id, city_name, country from {{ ref('dim_cities') }}
),

daily_agg as (
    select
        trip_date,
        city_id,
        trip_type,           -- corporate | personal

        count(*)                         as total_trips,
        sum(gross_revenue)               as gross_revenue,
        sum(net_revenue)                 as net_revenue,
        sum(processing_fee)              as total_processing_fees,
        sum(surge_revenue_contribution)  as surge_revenue,
        avg(surge_multiplier)            as avg_surge_multiplier,
        avg(trip_duration_minutes)       as avg_trip_duration_minutes,
        avg(gross_revenue)               as avg_fare,
        countif(is_extreme_surge)        as extreme_surge_trips,
        countif(has_duplicate_payment)   as duplicate_payment_trips,
        countif(is_fraud_suspect)        as fraud_suspect_trips

    from trips
    group by trip_date, city_id, trip_type
)

select
    {{ generate_surrogate_key_from_cols(['trip_date', 'city_id', 'trip_type']) }}
        as fct_daily_revenue_sk,

    da.trip_date,
    da.city_id,
    c.city_name,
    c.country,
    da.trip_type,
    da.total_trips,
    da.gross_revenue,
    da.net_revenue,
    da.total_processing_fees,
    da.surge_revenue,
    round(da.surge_revenue / nullif(da.gross_revenue, 0) * 100, 2)
        as surge_revenue_pct,
    da.avg_surge_multiplier,
    da.avg_trip_duration_minutes,
    da.avg_fare,
    da.extreme_surge_trips,
    da.duplicate_payment_trips,
    da.fraud_suspect_trips
from daily_agg da
left join cities c using (city_id)
