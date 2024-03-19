select
    sr.date_file as date_file
    , t.value:likes_for_client_HyPeHoUsE::number as likes
    , t.value:video_id_for_client_HyPeHoUsE::varchar as video_id
    , sr.country as country_code
    --, DATA_CONTENT:dislikes_for_client_HyPeHoUsE::number as dislikes
    --, DATA_CONTENT:index::int as index_likes

from {{ source('knowledge_provider_us', 'raw_likes_count_tab') }} sr, table(flatten(sr.$1,'data')) t