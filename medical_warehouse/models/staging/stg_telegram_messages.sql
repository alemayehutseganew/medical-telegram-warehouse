with ranked_messages as (
    select
        cast(message_id as bigint) as message_id,
        lower(trim(channel_name)) as channel_name,
        cast(message_date as timestamp) at time zone 'UTC' as message_ts,
        message_text,
        coalesce(has_media, false) as has_media,
        image_path,
        cast(views as integer) as view_count,
        cast(forwards as integer) as forward_count,
        row_number() over (partition by message_id order by message_date desc) as rn
    from {{ source('raw_layer', 'telegram_messages') }}
)

select
    message_id,
    channel_name,
    message_ts,
    message_text,
    length(coalesce(message_text, '')) as message_length,
    has_media as has_image,
    image_path,
    view_count,
    forward_count,
    date(message_ts) as message_date
from ranked_messages
where rn = 1
  and message_text is not null
  and message_text <> ''
