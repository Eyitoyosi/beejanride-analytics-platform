{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimensions']
    )
}}

/*
  dim_drivers
  ───────────
  Slowly Changing Dimension (Type 1 – current state).
  For historical changes use the drivers_snapshot snapshot.

  Joins driver profile data with lifetime performance metrics
  to produce a single enriched driver dimension.
*/

with drivers as (
    select * from {{ ref('stg_drivers') }}
),

cities as (
    select city_id, city_name, country from {{ ref('stg_cities') }}
),

lifetime_stats as (
    select * from {{ ref('int_driver_lifetime_stats') }}
),

final as (
    select
        d.driver_id,
        d.vehicle_id,
        d.city_id,
        c.city_name,
        c.country,
        d.driver_status,
        d.rating,
        d.onboarding_date,
        date_diff(current_date(), d.onboarding_date, month) as tenure_months,

        -- ── Lifetime metrics ──────────────────────────────────────
        coalesce(ls.lifetime_trips, 0)              as lifetime_trips,
        coalesce(ls.lifetime_gross_revenue, 0)      as lifetime_gross_revenue,
        coalesce(ls.lifetime_net_revenue, 0)        as lifetime_net_revenue,
        ls.avg_fare_per_trip,
        ls.avg_trip_duration_minutes,
        ls.first_trip_date,
        ls.last_trip_date,
        ls.corporate_trips,
        ls.personal_trips,
        ls.days_since_last_trip,
        ls.is_churned,
        ls.last_online_at,

        -- ── Derived tiers ─────────────────────────────────────────
        case
            when coalesce(ls.lifetime_trips, 0) >= 500  then 'platinum'
            when coalesce(ls.lifetime_trips, 0) >= 200  then 'gold'
            when coalesce(ls.lifetime_trips, 0) >= 50   then 'silver'
            else 'bronze'
        end as driver_tier,

        d.created_at,
        d.updated_at
    from drivers d
    left join cities        c  using (city_id)
    left join lifetime_stats ls using (driver_id)
)

select * from final
