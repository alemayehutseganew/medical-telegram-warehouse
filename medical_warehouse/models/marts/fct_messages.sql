with base as (
    select
        message_id,
        channel_name,
        message_ts,
        message_text,
        message_length,
        view_count,
        forward_count,
        has_image,
        date(message_ts) as dt
    from {{ ref('stg_telegram_messages') }}
)
select
    b.message_id,
    dc.channel_key,
    dd.date_key,
    b.message_text,
    b.message_length,
    b.view_count,
    b.forward_count,
    b.has_image
from base b
join {{ ref('dim_channels') }} dc
  on dc.channel_name = b.channel_name
join {{ ref('dim_dates') }} dd
  on dd.full_date = b.dt
