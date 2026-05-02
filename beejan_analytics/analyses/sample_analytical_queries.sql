-- ============================================================
-- BeejanRide – Sample Analytical Queries
-- Remember to run against the marts layer after dbt build completes.
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. Daily Revenue by City (last 30 days)
-- ────────────────────────────────────────────────────────────
select
    trip_date,
    city_name,
    sum(gross_revenue)  as gross_revenue,
    sum(net_revenue)    as net_revenue,
    sum(total_trips)    as total_trips,
    round(sum(net_revenue) / nullif(sum(gross_revenue), 0) * 100, 1) as net_margin_pct
from `beejanride-analytics.beejan_raw_marts_finance.fct_daily_revenue`
group by 1, 2
order by 1 desc, gross_revenue desc;


-- ────────────────────────────────────────────────────────────
-- 2. Gross vs Net Revenue – Corporate vs Personal 
-- ────────────────────────────────────────────────────────────
select
    trip_month_key,
    trip_type,
    sum(gross_revenue)                                       as gross_revenue,
    sum(net_revenue)                                         as net_revenue,
    sum(gross_revenue) - sum(net_revenue)                    as total_deductions,
    round(sum(net_revenue)/nullif(sum(gross_revenue),0)*100,1) as margin_pct
from `beejanride-analytics.beejan_raw_marts_core.fct_trips`
where trip_status = 'completed'
group by 1, 2
order by 1, 2;


-- ────────────────────────────────────────────────────────────
-- 3. Top 10 Drivers by Revenue (current month)
-- ────────────────────────────────────────────────────────────
select
    da.driver_id,
    d.city_name,
    d.driver_tier,
    d.driver_status,
    sum(da.gross_revenue)        as gross_revenue,
    sum(da.net_revenue)          as net_revenue,
    sum(da.trips_completed)      as trips_completed,
    round(avg(d.rating), 2)      as avg_rating,
    sum(da.online_hours)         as total_online_hours
from `beejanride-analytics.beejan_raw_marts_operations.fct_driver_activity` da
join `beejanride-analytics.beejan_raw_marts_core.dim_drivers` d using (driver_id)
group by 1, 2, 3, 4
order by gross_revenue desc
limit 10;


-- ────────────────────────────────────────────────────────────
-- 4. Rider Lifetime Value – Top Segments
-- ────────────────────────────────────────────────────────────
select
    rider_segment,
    count(*)                         as rider_count,
    round(avg(rider_ltv), 2)         as avg_ltv,
    round(avg(total_trips), 1)       as avg_trips,
    round(avg(days_since_last_trip)) as avg_days_since_last_trip,
    sum(rider_ltv)                   as segment_total_ltv
from beejanride-analytics.beejan_raw_marts_core.dim_riders
group by 1
order by avg_ltv desc;


-- ────────────────────────────────────────────────────────────
-- 5. Payment Failure Rate by Provider (last 7 days)
-- ────────────────────────────────────────────────────────────
select
    payment_date,
    payment_provider,
    total_attempts,
    successful_payments,
    failed_payments,
    round(payment_failure_rate_pct, 2)  as failure_rate_pct,
    failed_on_completed_trip,
    total_failed_amount                 as at_risk_revenue
from `beejanride-analytics.beejan_raw_marts_finance.fct_payment_reliability`
order by payment_date desc, payment_failure_rate_pct desc;


-- ────────────────────────────────────────────────────────────
-- 6. Surge Impact Analysis
-- ────────────────────────────────────────────────────────────
SELECT
    t.trip_date,
    c.city_name,  
    COUNT(*)                                         AS total_trips,
    COUNTIF(t.surge_multiplier > 1.0)               AS surge_trips,
    ROUND(COUNTIF(t.surge_multiplier > 1.0) / COUNT(*) * 100, 1)
                                                     AS surge_trip_pct,
    ROUND(AVG(CASE WHEN t.surge_multiplier > 1.0
                   THEN t.surge_multiplier END), 2) AS avg_surge_multiplier,
    SUM(t.surge_revenue_contribution)               AS total_surge_revenue,
    ROUND(SUM(t.surge_revenue_contribution)
          / NULLIF(SUM(t.gross_revenue), 0) * 100, 1) AS surge_revenue_pct
FROM `beejanride-analytics.beejan_raw_marts_core.fct_trips` t
LEFT JOIN `beejanride-analytics.beejan_raw_marts_core.dim_cities` c 
    ON t.city_id = c.city_id  -- adjust join condition as needed
WHERE t.trip_status = 'completed'
GROUP BY 1, 2
ORDER BY 1 DESC, total_surge_revenue DESC;


-- ────────────────────────────────────────────────────────────
-- 7. Driver Churn Tracking
-- ────────────────────────────────────────────────────────────
select
    city_name,
    driver_tier,
    driver_status,
    count(*)                                          as total_drivers,
    countif(is_churned)                               as churned_drivers,
    round(countif(is_churned)/count(*)*100, 1)        as churn_rate_pct,
    round(avg(days_since_last_trip))                  as avg_days_inactive
from `beejanride-analytics.beejan_raw_marts_core.dim_drivers`
group by 1, 2, 3
order by churn_rate_pct desc;


-- ────────────────────────────────────────────────────────────
-- 8. Fraud Detection – Top Suspect Trips
-- ────────────────────────────────────────────────────────────
select
    trip_date,
    fraud_category,
    count(*)                          as suspect_trips,
    sum(gross_revenue)                as at_risk_revenue,
    string_agg(distinct city_name)    as affected_cities
from `beejanride-analytics.beejan_raw_marts_fraud.fct_fraud_monitoring`
group by 1, 2
order by at_risk_revenue desc;


-- ────────────────────────────────────────────────────────────
-- 9. City Profitability Overview
-- ────────────────────────────────────────────────────────────
select
    c.city_name,
    c.country,
    c.months_since_launch,
    sum(r.gross_revenue)             as total_gross_revenue,
    sum(r.net_revenue)               as total_net_revenue,
    sum(r.total_trips)               as total_trips,
    round(avg(r.avg_fare), 2)        as avg_fare,
    sum(r.surge_revenue)             as total_surge_revenue,
    sum(r.fraud_suspect_trips)       as fraud_suspect_trips
from `beejanride-analytics.beejan_raw_marts_finance.fct_daily_revenue` r
join `beejanride-analytics.beejan_raw_marts_core.dim_cities` c using (city_id)
group by 1, 2, 3
order by total_net_revenue desc;


-- ────────────────────────────────────────────────────────────
-- 10. Driver Historical Status Changes (SCD2 snapshot query)
-- ────────────────────────────────────────────────────────────
select
    driver_id,
    driver_status,
    vehicle_id,
    rating,
    dbt_valid_from,
    dbt_valid_to,
    case when dbt_valid_to is null then 'current' else 'historical' end as record_type
from snapshots.drivers_snapshot
order by driver_id, dbt_valid_from;
