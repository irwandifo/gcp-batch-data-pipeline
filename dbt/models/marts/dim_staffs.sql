with

    stg_staffs as (select * from {{ source("pagila_silver", "staffs") }}),
    stg_stores as (select * from {{ source("pagila_silver", "stores") }}),
    stg_addresses as (select * from {{ source("pagila_silver", "addresses") }}),

    staffs as (
        select
            {{ dbt_utils.generate_surrogate_key(["stg_staffs.staff_id"]) }} as staff_sk,
            stg_staffs.staff_id as staff_nk,
            {{ dbt_utils.generate_surrogate_key(["stg_staffs.store_id"]) }} as store_sk,
            stg_staffs.name as staff_name,
            stg_staffs.email as staff_email,
            stg_addresses.phone as staff_phone,
            stg_addresses.address as staff_address,
            stg_addresses.zip_code as staff_zip_code,
            stg_addresses.district as staff_district,
            stg_addresses.city as staff_city,
            stg_addresses.country as staff_country,
            case
                when stg_stores.manager_staff_id is not null then true else false
            end as staff_is_manager,
            stg_staffs.is_active as staff_is_active
        from stg_staffs
        left join
            stg_stores
            on stg_staffs.staff_id = stg_stores.manager_staff_id
            and stg_staffs.store_id = stg_stores.store_id
        left join stg_addresses on stg_staffs.address_id = stg_addresses.address_id
    )

select *
from staffs
