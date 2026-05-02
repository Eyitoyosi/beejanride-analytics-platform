{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['driver_id'],
        tags=['staging', 'driver_events', 'incremental']
    )
}}

/*
  stg_driver_status_events
  ─────────────────────────
  Cleaned and deduplicated driver status change events.
  Source: beejan_raw.driver_status_events_raw

  This staging model performs the following transformations:
    - Deduplication of events based on event_id (keeping the latest in case of duplicates)
    - Standardization of driver_online_status values to lowercase and trimmed strings
    - Casting of event_timestamp to proper timestamp type
    - Extraction of event_date for partitioning

  Incremental materialization is used here since this is a high-volume event log that grows daily. 
  By partitioning on event_date, we can efficiently process only new events each day while keeping historical data intact.
    Tradeoffs:
        - Full refresh: always consistent, simpler logic, but higher cost and latency as data grows.
        - Incremental: lower cost and faster, but requires careful handling of late-arriving data and occasional full refreshes when logic changes.
    This model feeds into driver activity and lifetime stats models, which are queried frequently for operational dashboards and analytics.
*/

with source as (
    select * from {{ source('beejan_raw', 'driver_status_events_raw') }}

    {% if is_incremental() %}
        -- Only load events from the last processed partition onward
        -- Allows late-arriving data within a 1-day lookback window
        where cast(event_timestamp as date) >= date_sub(
            (select max(event_date) from {{ this }}),
            interval 1 day
        )
    {% endif %}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by event_id
        order by event_timestamp desc
    ) = 1
),

renamed as (
    select
        cast(event_id        as string)    as event_id,
        cast(driver_id       as string)    as driver_id,
        lower(trim(status))               as driver_online_status,
        cast(event_timestamp as timestamp) as event_timestamp,
        cast(event_timestamp as date)      as event_date    -- partition key

    from deduplicated
    where event_id is not null
)

select * from renamed
