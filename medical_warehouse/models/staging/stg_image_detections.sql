select
    cast(message_id as bigint) as message_id,
    lower(trim(channel_name)) as channel_name,
    image_path,
    label,
    cast(confidence as double precision) as confidence,
    image_category
from {{ source('raw_layer', 'image_detections') }}
where message_id is not null
