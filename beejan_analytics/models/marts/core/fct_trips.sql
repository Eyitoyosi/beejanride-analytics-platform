{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "trip_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['city_id', 'driver_id', 'trip_status'],
        tags=['marts', 'core', 'facts', 'incremental']
    )
}}

/*
  fct_trips
  ─────────
  Central fact table for all trip activity.
  Partitioned by trip_date (daily) for performant date-range queries.
  Clustered by city_id, driver_id, trip_status for common filter patterns.

  WHY INCREMENTAL:
    trips_raw is append-only for new trips. Running a full refresh daily
    would re-scan the entire trips history (potentially millions of rows).
    Incremental materialisation means we only process rows from the last
    completed partition (plus a 1-day lookback for late updates), reducing
    query cost and run time significantly.

  TRADEOFFS:
    Full refresh: always consistent, simpler logic, higher cost & latency.
    Incremental: lower cost & faster, but requires careful handling of
    late-arriving updates and occasional full refreshes when logic changes.
*/

with enriched as (
    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
        where trip_date >= date_sub(
            (select max(trip_date) from {{ this }}),
            interval 1 day
        )
    {% endif %}
)

select
    -- ── Surrogate key ─────────────────────────────────────────
    {{ generate_surrogate_key_from_cols(['trip_id']) }} as fct_trip_sk,
    trip_id,
    rider_id,
    driver_id,
    vehicle_id,
    city_id,
    payment_id,
    trip_date,
    trip_year,
    trip_month,
    trip_month_key,
    trip_status,
    payment_method,
    payment_status,
    payment_provider,
    trip_type,           -- It could be 'corporate' | 'personal'
    is_corporate,
    trip_duration_minutes,
    estimated_fare,
    actual_fare          as gross_revenue,
    net_revenue,
    fare_variance,
    processing_fee,
    payment_amount,
    surge_multiplier,
    surge_revenue_contribution,
    is_extreme_surge,
    has_duplicate_payment,
    is_completed_without_payment,
    is_fraud_suspect,
    failed_payment_count,
    success_payment_count,
    requested_at,
    pickup_at,
    dropoff_at,
    payment_created_at,
    created_at,
    updated_at

from enriched
