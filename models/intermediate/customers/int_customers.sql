with customers as (

    select
        {{ dbt_utils.star(
            from=ref('stg_customers'),
            except=['created_at']
        ) }},
        created_at as customer_created_at
    from {{ ref('stg_customers') }}

),

customers_standardized as (

    select
        c.*,
        coalesce(m.standard_country, c.country) as country_standardized
    from customers c
    left join {{ ref('country_mapping') }} m
        on c.country = m.raw_country

)

select *
from customers_standardized
