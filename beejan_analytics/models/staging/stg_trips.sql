{{
    config(
        materialized='view',
        tags=['staging', 'trips']
    )
}}

/*
  ───────────────────────────────
  Staging model for trips data.
  Performs basic cleaning, type casting, and deduplication of the raw trips data ingested from the source system.

  This model serves as the single source of truth for trip-level data in our warehouse, and is used by multiple downstream models that calculate metrics such as:
     - trip counts and revenue per rider/driver
     - trip lifecycle metrics (e.g. time to pickup, trip duration)
        - payment success rates and failure reasons
    View materialization is used here since the transformations are relatively lightweight and the data volume is manageable, 
    allowing for flexibility in refreshing the data without needing to manage storage or incremental logic.
*/

with source as (
    select * from {{ source('beejan_raw', 'trips_raw') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by trip_id
        order by updated_at desc
    ) = 1
),

renamed as (
    select
        cast(trip_id      as string)  as trip_id,
        cast(rider_id     as string)  as rider_id,
        cast(driver_id    as string)  as driver_id,
        cast(vehicle_id   as string)  as vehicle_id,
        cast(city_id      as string)  as city_id,
        cast(requested_at as timestamp) as requested_at,
        cast(pickup_at    as timestamp) as pickup_at,
        cast(dropoff_at   as timestamp) as dropoff_at,
        cast(created_at   as timestamp) as created_at,
        cast(updated_at   as timestamp) as updated_at,
        lower(trim(status))          as trip_status,
        lower(trim(payment_method))  as payment_method,
        cast(is_corporate as boolean) as is_corporate,
        cast(estimated_fare   as numeric) as estimated_fare,
        cast(actual_fare      as numeric) as actual_fare,
        cast(surge_multiplier as numeric) as surge_multiplier

    from deduplicated
    where trip_id is not null
)

select * from renamed