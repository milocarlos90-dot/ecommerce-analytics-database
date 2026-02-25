with orders as (

    select *
    from {{ ref('stg_orders') }}

)

select
    o.*,

    created_at as order_created_at,
    date(created_at) as order_created_date,

    -- lifecycle flags
    status = 'Complete'   as is_completed_order,
    status = 'Cancelled'  as is_cancelled_order,
    status = 'Returned'   as is_returned_order,
    status = 'Shipped'    as is_shipped_order,
    status = 'Processing' as is_processing_order

from orders o
