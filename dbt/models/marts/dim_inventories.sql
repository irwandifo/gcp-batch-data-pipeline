with

    stg_inventories as (select * from {{ source("pagila_silver", "inventories") }}),

    inventories as (
        select
            {{ dbt_utils.generate_surrogate_key(["stg_inventories.inventory_id"]) }} as inventory_sk,
            {{ dbt_utils.generate_surrogate_key(["stg_inventories.film_id"]) }} as film_sk,
            {{ dbt_utils.generate_surrogate_key(["stg_inventories.store_id"]) }} as store_sk,
            stg_inventories.inventory_id as inventory_nk
        from stg_inventories
    )

select *
from inventories
