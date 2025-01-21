with stg_stores as (
    select *
    from {{ ref('stores') }}
),

stg_addresses as (
    select *
    from {{ ref('addresses') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['stg_stores.store_id']) }} as store_sk,
        stg_stores.store_id,
        stg_staffs.name as store_manager,
        stg_address.phone as store_phone,
        stg_address.address as store_address,
        stg_address.zip_code as store_zip_code,
        stg_address.district as store_district,
        stg_address.city as store_city,
        stg_address.country as store_country
    from stg_stores
    left join stg_addresses on stg_stores.address_id = stg_addresses.address_id
    left join stg_staffs on stg_stores.manager_staff_id = stg_staffs.staff_id
)

select * from final
