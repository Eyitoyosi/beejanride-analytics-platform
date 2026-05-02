{{
    config(
        materialized='view',
        tags=['staging', 'payments']
    )
}}

with source as (
    select * from {{ source('beejan_raw', 'payments_raw') }}
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by payment_id
        order by created_at desc
    ) = 1
),

renamed as (
    select
        cast(payment_id       as string)    as payment_id,
        cast(trip_id          as string)    as trip_id,
        lower(trim(payment_status))         as payment_status,
        lower(trim(payment_provider))       as payment_provider,
        cast(amount           as numeric)   as amount,
        cast(fee              as numeric)   as processing_fee,
        upper(trim(currency))               as currency,
        cast(created_at       as timestamp) as created_at

    from deduplicated
    where payment_id is not null
)

select * from renamed
