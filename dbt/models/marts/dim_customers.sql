with

    stg_customers as (select * from {{ source("pagila_silver", "customers") }}),
    stg_addresses as (select * from {{ source("pagila_silver", "addresses") }}),

    customers as (
        select
            {{ dbt_utils.generate_surrogate_key(["stg_customers.customer_id"]) }} as customer_sk,
            stg_customers.customer_id as customer_nk,
            {{ dbt_utils.generate_surrogate_key(["stg_customers.store_id"]) }} as store_sk,
            stg_customers.name as customer_name,
            stg_customers.email as customer_email,
            stg_addresses.phone as customer_phone,
            stg_addresses.zip_code as customer_zip_code,
            stg_addresses.district as customer_district,
            stg_addresses.city as customer_city,
            stg_addresses.country as customer_country,
            stg_customers.is_active as customer_is_active,
            stg_customers.created_at,
            stg_customers.updated_at,
            stg_customers.loaded_at
        from stg_customers
        left join stg_addresses on stg_customers.address_id = stg_addresses.address_id
    )

select *
from customers
