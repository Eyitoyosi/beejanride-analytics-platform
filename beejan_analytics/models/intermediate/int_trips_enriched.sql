{{
    config(
        materialized='table',
        tags=['intermediate', 'trips']
    )
}}

/*
  int_trips_enriched
  ──────────────────
  Joins trips to payments, cities, drivers and riders to produce
  a single enriched trip record used by multiple downstream marts.

  Calculates:
    - trip_duration_minutes
    - net_revenue (actual_fare minus platform fee and payment processing fee)
    - corporate_trip_flag
    - surge impact metrics
    - fraud indicator flags

 Table materialization is used here since this is a wide denormalized table that is queried by multiple downstream models, 
 and we want to avoid recomputing the joins and calculations each time.
*/

with trips as (
    select * from {{ ref('stg_trips') }}
),

payments as (
    -- Latest successful payment per trip (a trip may have retries)
    select
        trip_id,
        payment_id,
        payment_status,
        payment_provider,
        amount           as payment_amount,
        processing_fee,
        currency,
        created_at       as payment_created_at,
        row_number() over (
            partition by trip_id
            order by
                case payment_status when 'success' then 0 else 1 end,
                created_at desc
        ) as rn
    from {{ ref('stg_payments') }}
),

latest_payment as (
    select * from payments where rn = 1
),

-- Aggregate: did this trip have ANY failed payment?
payment_failures as (
    select
        trip_id,
        countif(payment_status = 'failed')  as failed_payment_count,
        countif(payment_status = 'success') as success_payment_count
    from {{ ref('stg_payments') }}
    group by trip_id
),

-- Duplicate payment detection: more than one successful payment
duplicate_payments as (
    select
        trip_id,
        count(*) as duplicate_payment_count
    from {{ ref('stg_payments') }}
    where payment_status = 'success'
    group by trip_id
    having count(*) > 1
),

cities as (
    select * from {{ ref('stg_cities') }}
),

drivers as (
    select * from {{ ref('stg_drivers') }}
),

riders as (
    select * from {{ ref('stg_riders') }}
),

enriched as (
    select
        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.vehicle_id,
        t.city_id,
        t.requested_at,
        t.pickup_at,
        t.dropoff_at,
        t.created_at,
        t.updated_at,
        cast(t.requested_at as date)                     as trip_date,
        extract(year  from t.requested_at)               as trip_year,
        extract(month from t.requested_at)               as trip_month,
        format_date('%Y-%m', cast(t.requested_at as date)) as trip_month_key,
        t.trip_status,
        t.payment_method,
        -- ── Duration ─────────────────────────────────────────────
        {{ calc_duration_minutes('t.pickup_at', 't.dropoff_at') }}
            as trip_duration_minutes,

        -- ── Revenue ──────────────────────────────────────────────
        t.estimated_fare,
        t.actual_fare,
        {{ calc_net_revenue('t.actual_fare', 'p.processing_fee') }}
            as net_revenue,
        t.actual_fare - t.estimated_fare                 as fare_variance,

        -- ── Surge ────────────────────────────────────────────────
        t.surge_multiplier,
        case when t.surge_multiplier > 1.0
             then t.actual_fare - (t.actual_fare / t.surge_multiplier)
             else 0
        end                                              as surge_revenue_contribution,

        -- ── Corporate flag ───────────────────────────────────────
        t.is_corporate,
        case when t.is_corporate then 'corporate' else 'personal'
        end                                              as trip_type,
        p.payment_id,
        p.payment_status,
        p.payment_provider,
        p.payment_amount,
        p.processing_fee,
        p.currency,
        p.payment_created_at,

        coalesce(pf.failed_payment_count, 0)             as failed_payment_count,
        coalesce(pf.success_payment_count, 0)            as success_payment_count,

        c.city_name,
        c.country                                        as city_country,
        d.driver_status,
        d.rating                                         as driver_rating,
        r.signup_date                                    as rider_signup_date,
        r.country                                        as rider_country,
        r.referral_code,

        -- ── Fraud indicators ─────────────────────────────────────
        case when t.surge_multiplier > {{ var('surge_extreme_threshold') }}
             then true else false
        end                                              as is_extreme_surge,

        case when dp.trip_id is not null
             then true else false
        end                                              as has_duplicate_payment,

        case
            when t.trip_status = 'completed'
             and coalesce(pf.success_payment_count, 0) = 0
            then true
            else false
        end                                              as is_completed_without_payment,

        -- Overall fraud flag (any indicator triggered)
        case
            when t.surge_multiplier > {{ var('surge_extreme_threshold') }}
              or dp.trip_id is not null
              or (t.trip_status = 'completed'
                  and coalesce(pf.success_payment_count, 0) = 0)
            then true
            else false
        end                                              as is_fraud_suspect

    from trips          t
    left join latest_payment  p  on t.trip_id = p.trip_id
    left join payment_failures pf on t.trip_id = pf.trip_id
    left join duplicate_payments dp on t.trip_id = dp.trip_id
    left join cities    c  on t.city_id = c.city_id
    left join drivers   d  on t.driver_id = d.driver_id
    left join riders    r  on t.rider_id = r.rider_id
)

select * from enriched
