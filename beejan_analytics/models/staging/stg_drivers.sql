{{
    config(
        materialized='view',
        tags=['staging', 'drivers']
    )
}}

with source as (
    select * from {{ source('beejan_raw', 'drivers_raw') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by driver_id
        order by updated_at desc
    ) = 1
),

renamed as (
    select
        cast(driver_id    as string)   as driver_id,
        cast(vehicle_id   as string)   as vehicle_id,
        cast(city_id      as string)   as city_id,
        lower(trim(driver_status))     as driver_status,
        cast(rating       as numeric)  as rating,
        cast(onboarding_date as date)  as onboarding_date,
        cast(created_at   as timestamp) as created_at,
        cast(updated_at   as timestamp) as updated_at

    from deduplicated
    where driver_id is not null
)

select * from renamed
