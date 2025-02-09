{{ config(
    materialized = "incremental",
    partition_by = {"field": "rented_at", "data_type": "timestamp"},
    incremental_strategy = "insert_overwrite"
  )
}}

with

    stg_rentals as (select * from {{ source("pagila_silver", "rentals") }}),
    stg_staffs as (select * from {{ source("pagila_silver", "staffs") }}),
  
    rentals as (
        select
            stg_rentals.rental_id,
            stg_rentals.inventory_id,
            stg_rentals.customer_id,
            {{ dbt_utils.generate_surrogate_key(["stg_rentals.staff_id"]) }} as staff_sk,
            stg_rentals.rented_at,
            stg_rentals.returned_at,
            stg_rentals.updated_at,
            stg_rentals.loaded_at
        from stg_rentals
        left join stg_staffs on stg_rentals.staff_id = stg_staffs.staff_id

        {% if is_incremental() %}
  
          where date(rented_at) >= date_sub(date(_dbt_max_partition), interval 1 day)
  
        {% endif %}
    )

select *
from rentals
