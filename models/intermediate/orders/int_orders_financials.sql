select
    order_id,

    count(*) as item_count,
    count(distinct product_id) as distinct_products,

    sum(sale_price) as gross_revenue,
    sum(if(status = 'Complete',   sale_price, 0)) as completed_revenue,
    sum(if(status = 'Cancelled',  sale_price, 0)) as cancelled_revenue,
    sum(if(status = 'Returned',   sale_price, 0)) as returned_revenue,
    sum(if(status = 'Shipped',    sale_price, 0)) as shipped_revenue,
    sum(if(status = 'Processing', sale_price, 0)) as processing_revenue

from {{ ref('stg_order_items') }}
group by order_id