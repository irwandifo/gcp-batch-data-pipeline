{{ config(
    materialized = "incremental",
    partition_by = {"field": "paid_at", "data_type": "timestamp"},
    incremental_strategy = "insert_overwrite"
  )
}}

with

    stg_payments as (select * from {{ source("pagila_silver", "payments") }}),
    stg_staffs as (select * from {{ source("pagila_silver", "staffs") }}),
  
    payments as (
        select
            stg_payments.payment_id,
            stg_payments.rental_id,
            stg_payments.customer_id,
            {{ dbt_utils.generate_surrogate_key(["stg_payments.staff_id"]) }} as staff_sk,
            stg_payments.amount,
            stg_payments.paid_at,
            stg_payments.loaded_at
        from stg_payments
        left join stg_staffs on stg_payments.staff_id = stg_staffs.staff_id

        {% if is_incremental() %}
  
          where date(event_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
  
        {% endif %}
    )

select *
from payments
