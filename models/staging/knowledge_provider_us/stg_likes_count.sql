select
    index as index_likes
    , likes
    , dislikes
    , video_id

from {{ source('knowledge_provider_us', 'likes_count_tab') }}