with base as (
    select
        channel_name,
        min(message_ts) as first_post_date,
        max(message_ts) as last_post_date,
        count(*) as total_posts,
        avg(view_count)::integer as avg_views
    from {{ ref('stg_telegram_messages') }}
    group by channel_name
)

select
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} as channel_key,
    channel_name,
    case
        when channel_name ilike '%cosmetic%' then 'Cosmetics'
        when channel_name ilike '%pharma%' then 'Pharmaceutical'
        else 'Medical'
    end as channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
from base
