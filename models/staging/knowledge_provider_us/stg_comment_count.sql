select
    index as index_comment,
    comment_count,
    video_id

from {{ source('knowledge_provider_us', 'comment_count_tab') }}