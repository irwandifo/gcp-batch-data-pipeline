with

    stg_payments as (select * from {{ source("pagila_silver", "payments") }}),
    stg_rentals as (select * from {{ source("pagila_silver", "rentals") }}),
    stg_inventories as (select * from {{ source("pagila_silver", "inventories") }}),
  
    sales as (
        select
            {{ dbt_utils.generate_surrogate_key(["stg_payments.payment_id","stg_payments.rental_id"]) }} as sales_sk,
            {{ dbt_utils.generate_surrogate_key(["stg_payments.rental_id"]) }} as rental_sk,
            {{ dbt_utils.generate_surrogate_key(["stg_rentals.inventory_id"]) }} as inventory_sk,
            {{ dbt_utils.generate_surrogate_key(["stg_payments.customer_id"]) }} as customer_sk,
            {{ dbt_utils.generate_surrogate_key(["stg_payments.staff_id"]) }} as staff_sk,
            {{ dbt_utils.generate_surrogate_key(["stg_inventories.store_id"]) }} as store_sk,
            stg_payments.payment_id as sales_nk,
            stg_payments.rental_id as rental_nk,
            stg_payments.amount as sales_amount,
            stg_payments.paid_at
            stg_rentals.rented_at,
            stg_rentals.returned_at
        from stg_payments
        left join stg_rentals on stg_payments.rental_id = stg_rentals.rental_id
        left join stg_inventories on stg_rentals.inventory_id = stg_inventories.inventory_id
    )

select *
from sales
