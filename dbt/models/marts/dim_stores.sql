with

    stg_stores as (select * from {{ source("pagila_silver", "stores") }}),
    stg_staffs as (select * from {{ source("pagila_silver", "staffs") }}),
    stg_addresses as (select * from {{ source("pagila_silver", "addresses") }}),

    stores as (
        select
            {{ dbt_utils.generate_surrogate_key(["stg_stores.store_id"]) }} as store_sk,
            stg_stores.store_id as store_nk,
            stg_staffs.name as store_manager,
            stg_addresses.phone as store_phone,
            stg_addresses.address as store_address,
            stg_addresses.zip_code as store_zip_code,
            stg_addresses.district as store_district,
            stg_addresses.city as store_city,
            stg_addresses.country as store_country
        from stg_stores
        left join stg_staffs on stg_stores.manager_staff_id = stg_staffs.staff_id
        left join stg_addresses on stg_stores.address_id = stg_addresses.address_id
    )

select *
from stores
