-- ──────────────────────────────────────────────────────────────────
-- Custom Generic Test: assert_no_negative_revenue
--
-- Fails if any row has a negative value in the tested column.
-- Usage in YAML:
--   - name: gross_revenue
--     tests:
--       - assert_no_negative_revenue
-- ──────────────────────────────────────────────────────────────────
{% test assert_no_negative_revenue(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}


-- ──────────────────────────────────────────────────────────────────
-- Custom Generic Test: assert_positive_trip_duration
--
-- Fails if trip_duration_minutes <= 0 for completed trips.
-- ──────────────────────────────────────────────────────────────────
{% test assert_positive_trip_duration(model, column_name) %}

select *
from {{ model }}
where trip_status = 'completed'
  and ({{ column_name }} is null or {{ column_name }} <= 0)

{% endtest %}


-- ──────────────────────────────────────────────────────────────────
-- Custom Generic Test: assert_completed_trip_has_payment
--
-- Fails if a completed trip has zero successful payments.
-- Catches revenue leakage scenarios.
-- ──────────────────────────────────────────────────────────────────
{% test assert_completed_trip_has_payment(model, column_name) %}

select *
from {{ model }}
where trip_status = 'completed'
  and {{ column_name }} = 0

{% endtest %}
