with customers as (

    select
        {{ dbt_utils.star(
            from=ref('stg_customers'),
            except=[
                'created_at',
                'first_name',
                'last_name',
                'email',
                'gender',
                'state',
                'street_address',
                'postal_code',
                'city',
                'country',
                'traffic_source'
            ]
        ) }},
        
        created_at as customer_created_at,

        -- clean process nulls see clean text macro for more details
        initcap({{ clean_text('first_name') }}) as first_name,
        initcap({{ clean_text('last_name') }}) as last_name,
        lower({{ clean_text('email') }}) as email,
        case
          when {{ clean_text('gender') }} = 'M' then 'Male'
          when {{ clean_text('gender') }} = 'F' then 'Female'
        end as gender,
        initcap({{ clean_text('state') }}) as state,
        ({{ clean_text('street_address') }}) as street_address,
        upper({{ clean_text('postal_code') }}) as postal_code,
        initcap({{ clean_text('city') }}) as city,
        initcap({{ clean_text('country') }}) as country,
        lower({{ clean_text('traffic_source') }}) as traffic_source
        
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