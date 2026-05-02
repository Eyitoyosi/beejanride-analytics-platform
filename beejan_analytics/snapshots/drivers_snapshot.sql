{% snapshot drivers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='driver_id',
        strategy='timestamp',
        updated_at='updated_at',
        tags=['snapshot', 'drivers', 'scd2']
    )
}}

/*
  drivers_snapshot  –  SCD Type 2
  ─────────────────────────────────
  Tracks historical changes to:
    - driver_status  (active → suspended → inactive)
    - vehicle_id     (vehicle change)
    - rating         (rating updates)


*/

select
    driver_id,
    vehicle_id,
    city_id,
    driver_status,
    rating,
    onboarding_date,
    created_at,
    updated_at
from {{ ref('stg_drivers') }}

{% endsnapshot %}