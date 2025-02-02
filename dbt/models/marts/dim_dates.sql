with
    calendar as (
        select
            dates as date_key,
            dates,
            extract(day from dates) as day,
            extract(month from dates) as month,
            extract(year from dates) as year,
            extract(quarter from dates) as quarter,
            extract(dayofweek from dates) as day_of_week,
            case
                when extract(dayofweek from dates) in (0, 6) then true else false
            end as is_weekend
        from unnest(generate_date_array('2022-01-01', '2022-12-31')) as dates
    )

select *
from calendar
