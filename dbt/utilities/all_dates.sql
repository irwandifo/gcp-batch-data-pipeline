with calendar as (
    select 
        date('2022-01-01') as date_day
    union all
    select 
        date_add(date_day, interval 1 day)
    from 
        calendar
    where 
        date_day < date('2022-12-31')
)

select
    date_day as date,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(week from date_day) as week,
    extract(quarter from date_day) as quarter,
    format_date('%a', date_day) as day_name,
    format_date('%b', date_day) as month_name,
    case 
        when format_date('%a', date_day) in ('saturday', 'sunday') then true 
        else false 
    end as is_weekend
from 
    calendar
