{{
    config(
        materialized='view',
        tags=['staging', 'riders']
    )
}}

/*
  stg_riders

  This staging model performs basic cleaning and transformations on the raw riders data, including:
    - Deduplication to keep the latest record per rider_id
    - Standardizing data types and formats
    - Basic validation to filter out records with missing rider_id

  This model serves as the clean, standardized source of rider data for all downstream models.
    View materialization is used here since the transformations are relatively lightweight and the data volume is manageable, allowing for flexibility in refreshing the data without needing to manage storage or incremental logic.
    If the data volume grows significantly in the future, we may consider switching to a table materialization for better performance at the cost of storage and refresh complexity.
*/

with source as (
    select * from {{ source('beejan_raw', 'riders_raw') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by rider_id
        order by created_at desc
    ) = 1
),

renamed as (
    select
        cast(rider_id     as string)    as rider_id,
        cast(signup_date  as date)      as signup_date,
        upper(trim(country))            as country,
        cast(referral_code as string)   as referral_code,
        cast(created_at   as timestamp) as created_at

    from deduplicated
    where rider_id is not null
)

select * from renamed
