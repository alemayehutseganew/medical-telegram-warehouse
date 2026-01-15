with dates as (
    select
        generate_series(
            (select date(min(message_ts)) from {{ ref('stg_telegram_messages') }}),
            (select date(max(message_ts)) from {{ ref('stg_telegram_messages') }}),
            interval '1 day'
        ) as calendar_date
)
select
    to_char(calendar_date, 'YYYYMMDD')::bigint as date_key,
    calendar_date as full_date,
    extract(dow from calendar_date)::integer as day_of_week,
    to_char(calendar_date, 'Day') as day_name,
    extract(week from calendar_date)::integer as week_of_year,
    extract(month from calendar_date)::integer as month,
    to_char(calendar_date, 'Month') as month_name,
    extract(quarter from calendar_date)::integer as quarter,
    extract(year from calendar_date)::integer as year,
    case when extract(dow from calendar_date) in (0, 6) then true else false end as is_weekend
from dates
