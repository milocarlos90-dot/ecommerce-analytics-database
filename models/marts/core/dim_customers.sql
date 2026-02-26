with customers as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        age,
        gender,
        street_address,
        postal_code,
        city,
        state,
        country,
        country_standardized,
        latitude,
        longitude,
        customer_geography,
        traffic_source,
        customer_created_at
    from {{ ref('int_customers') }}

),

final as (

    select
        -- Keys
        customer_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,

        -- Identity
        first_name,
        last_name,
        concat(first_name, ' ', last_name) as full_name,
        email,

        -- Demographics
        age,
        gender,
        case
            when age < 25 then '18-24'
            when age < 35 then '25-34'
            else '35+'
        end as age_group,

        -- Location
        coalesce(street_address, 'Unknown') as street_address,
        coalesce(postal_code, 'Unknown') as postal_code,
        coalesce(city, 'Unknown') as city,
        coalesce(state, 'Unknown') as state,
        coalesce(country, 'Unknown') as country,
        coalesce(country_standardized, 'Unknown') as country_standardized,

        -- Geography
        latitude,
        longitude,
        customer_geography,

        -- Acquisition
        traffic_source,

        customer_created_at

    from customers

)

select *
from final