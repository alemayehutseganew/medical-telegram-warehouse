select
    dc.channel_key,
    dd.date_key,
    sm.message_id,
    sid.image_path,
    sid.label as detected_class,
    sid.confidence as confidence_score,
    sid.image_category
from {{ ref('stg_image_detections') }} sid
join {{ ref('stg_telegram_messages') }} sm
  on sm.message_id = sid.message_id
join {{ ref('dim_channels') }} dc
  on dc.channel_name = sm.channel_name
join {{ ref('dim_dates') }} dd
  on dd.full_date = date(sm.message_ts)
