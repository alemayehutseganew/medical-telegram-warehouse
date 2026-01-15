select
  message_id,
  message_ts
from {{ ref('stg_telegram_messages') }}
where message_ts > current_timestamp
