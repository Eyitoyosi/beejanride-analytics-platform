{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'payments']
    )
}}

/*
  fct_payment_reliability
  ───────────────────────
  Daily payment success/failure metrics per provider.
  Powers the Payment Reliability Report.
*/

with payments as (
    select * from {{ ref('stg_payments') }}
),

trips as (
    select trip_id, city_id, trip_status, trip_date
    from {{ ref('fct_trips') }}
),

joined as (
    select
        cast(p.created_at as date)       as payment_date,
        t.city_id,
        p.payment_provider,
        p.currency,
        p.payment_status,
        p.amount,
        p.processing_fee,
        t.trip_status
    from payments p
    left join trips t using (trip_id)
),

aggregated as (
    select
        payment_date,
        city_id,
        payment_provider,
        currency,
        count(*)                                      as total_attempts,
        countif(payment_status = 'success')           as successful_payments,
        countif(payment_status = 'failed')            as failed_payments,

        -- Failed payments on COMPLETED trips = revenue leakage
        countif(payment_status = 'failed'
                and trip_status = 'completed')        as failed_on_completed_trip,

        sum(case when payment_status = 'success'
                 then amount else 0 end)              as total_collected,
        sum(case when payment_status = 'failed'
                 then amount else 0 end)              as total_failed_amount,
        sum(case when payment_status = 'success'
                 then processing_fee else 0 end)      as total_fees
    from joined
    group by payment_date, city_id, payment_provider, currency
)

select
    {{ generate_surrogate_key_from_cols(['payment_date', 'city_id', 'payment_provider', 'currency']) }}
        as fct_payment_reliability_sk,

    *,

    -- Failure rate
    {{ safe_divide('failed_payments', 'total_attempts') }} * 100
        as payment_failure_rate_pct,

    -- Net collected after fees
    total_collected - total_fees  as net_collected

from aggregated
