select
    index as video_index
    , video_id
    , title
    , description
    , dislikes
    , published_at
    , trending_date
    , category_id
    , channel_id
    , channel_title
    , comments_disabled
    , ratings_disabled
    , tags
    , thumbnail_link
    , garbled_1
    , garbled_2
    , garbled_3
    , garbled_4
    , garbled_5
   
from {{ source('knowledge_provider_us', 'video_info_tab') }}