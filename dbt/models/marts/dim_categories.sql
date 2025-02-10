with

    stg_categories as (select * from {{ source("pagila_silver", "categories") }}),

    categories as (
        select
           {{ dbt_utils.generate_surrogate_key(["stg_categories.category_id"]) }} as category_sk,
            stg_categories.category_id as category_nk,
            stg_categories.name as category_name
        from stg_categories
    )

select *
from categories
