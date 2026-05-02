-- ──────────────────────────────────────────────────────────────
-- Macro: calc_net_revenue
-- Purpose: Calculates net revenue after platform fee and payment fees.
-- 
-- Formula:
--   net_revenue = actual_fare
--               - (actual_fare * platform_fee_pct)   ← BeejanRide cut
--               - processing_fee                      ← payment provider

{% macro calc_net_revenue(fare_col, processing_fee_col) %}
    round(
        coalesce({{ fare_col }}, 0)
        - (coalesce({{ fare_col }}, 0) * {{ var('platform_fee_pct') }})
        - coalesce({{ processing_fee_col }}, 0),
        2
    )
{% endmacro %}


-- ──────────────────────────────────────────────────────────────
-- Macro: calc_duration_minutes
-- Purpose: Returns duration in decimal minutes between two timestamps.
--          Returns NULL if either timestamp is NULL or result is <= 0.

{% macro calc_duration_minutes(start_ts, end_ts) %}
    nullif(
        round(
            timestamp_diff({{ end_ts }}, {{ start_ts }}, second) / 60.0,
            2
        ),
        0
    )
{% endmacro %}


-- ──────────────────────────────────────────────────────────────
-- Macro: cents_to_pounds
-- Purpose: Converts integer pence values to GBP pounds with 2dp.

-- ──────────────────────────────────────────────────────────────
{% macro cents_to_pounds(amount_col) %}
    round(coalesce({{ amount_col }}, 0) / 100.0, 2)
{% endmacro %}


-- ──────────────────────────────────────────────────────────────
-- Macro: safe_divide
-- Purpose: Division that returns NULL instead of dividing by zero.

{% macro safe_divide(numerator, denominator) %}
    case
        when coalesce({{ denominator }}, 0) = 0 then null
        else round({{ numerator }} / {{ denominator }}, 4)
    end
{% endmacro %}


-- ──────────────────────────────────────────────────────────────
-- Macro: generate_surrogate_key_from_cols
-- Purpose: Thin wrapper around dbt_utils.generate_surrogate_key
--          to keep our models cleaner.

{% macro generate_surrogate_key_from_cols(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
