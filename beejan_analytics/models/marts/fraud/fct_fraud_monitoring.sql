{{
    config(
        materialized='table',
        tags=['marts', 'fraud']
    )
}}

/*
  fct_fraud_monitoring
  ─────────────────────
  Fraud detection view surfacing all suspect trips.
  Tracks three indicators:
    1. extreme_surge      – surge multiplier > 10x
    2. duplicate_payment  – more than one successful payment for a trip
    3. completed_no_pay   – completed trip with zero successful payments

  Used by the Fraud Monitoring Dashboard and Ops alerts.
*/

with trips as (
    select * from {{ ref('fct_trips') }}
    where is_fraud_suspect = true
),

drivers as (
    select driver_id, city_name, driver_tier, driver_status
    from {{ ref('dim_drivers') }}
),

riders as (
    select rider_id, rider_segment, total_trips as rider_total_trips
    from {{ ref('dim_riders') }}
)

select
    t.trip_id,
    t.trip_date,
    t.city_id,
    t.driver_id,
    t.rider_id,
    t.trip_status,
    t.gross_revenue,
    t.net_revenue,
    t.surge_multiplier,
    t.payment_method,
    t.payment_status,
    t.is_extreme_surge,
    t.has_duplicate_payment,
    t.is_completed_without_payment,

    -- Fraud category label
    case
        when t.is_extreme_surge and t.has_duplicate_payment then 'multiple_indicators'
        when t.is_extreme_surge                             then 'extreme_surge'
        when t.has_duplicate_payment                        then 'duplicate_payment'
        when t.is_completed_without_payment                 then 'completed_no_payment'
        else 'other'
    end as fraud_category,
    d.city_name,
    d.driver_tier,
    d.driver_status,
    r.rider_segment,
    r.rider_total_trips,
    t.requested_at,
    t.payment_created_at

from trips t
left join drivers d using (driver_id)
left join riders  r using (rider_id)
