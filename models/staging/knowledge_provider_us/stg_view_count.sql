select
    index as index_view
    , view_count
    , video_id

FROM {{ source('knowledge_provider_us', 'view_count_tab') }}