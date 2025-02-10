with

    stg_actors as (select * from {{ source("pagila_silver", "actors") }}),

    actors as (
        select
            {{ dbt_utils.generate_surrogate_key(["stg_actors.actor_id"]) }} as actor_sk,
            stg_actors.actor_id as actor_nk,
            stg_actors.name as actor_name
        from stg_actors
    )

select *
from actors
