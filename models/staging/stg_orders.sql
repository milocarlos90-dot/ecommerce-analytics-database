{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('thelook_ecommerce', 'orders') }}

),

renamed as (

    select
        cast(order_id as int64)           as order_id,
        cast(user_id as int64)            as customer_id,
        cast(status as string)            as status,
        cast(gender as string)            as gender,
        cast(created_at as timestamp)     as created_at,
        cast(returned_at as timestamp)    as returned_at,
        cast(shipped_at as timestamp)     as shipped_at,
        cast(delivered_at as timestamp)   as delivered_at,
        cast(num_of_item as int64)        as number_of_items
    from source

)

select *
from renamed
