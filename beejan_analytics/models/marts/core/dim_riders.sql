{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

with riders as (
    select * from {{ ref('stg_riders') }}
),

ltv as (
    select * from {{ ref('int_rider_lifetime_value') }}
),

final as (
    select
        r.rider_id,
        r.signup_date,
        r.country,
        r.referral_code,

        -- LTV & engagement
        coalesce(l.total_trips, 0)          as total_trips,
        coalesce(l.total_gross_spend, 0)    as total_gross_spend,
        coalesce(l.rider_ltv, 0)            as rider_ltv,
        l.avg_fare,
        l.first_trip_date,
        l.last_trip_date,
        l.days_since_last_trip,
        l.corporate_trips,
        l.signup_cohort,
        l.rider_segment,
        l.is_referred,

        r.created_at

    from riders r
    left join ltv l using (rider_id)
)

select * from final
