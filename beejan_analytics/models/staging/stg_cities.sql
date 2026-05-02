{{
    config(
        materialized='view',
        tags=['staging', 'cities']
    )
}}

with source as (
    select * from {{ source('beejan_raw', 'cities_raw') }}
),

renamed as (
    select
        cast(city_id      as string) as city_id,
        initcap(city_name)           as city_name,
        upper(trim(country))         as country,
        cast(launch_date  as date)   as launch_date

    from source
    where city_id is not null
)

select * from renamed
