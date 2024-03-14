select
    sr.date_file as date_file
    , t.value:video_id_for_client_HyPeHoUsE::varchar as video_id
    --, DATA_CONTENT:index::int as index_video
    , t.value:categoryId_for_client_HyPeHoUsE::number as category_id
    , t.value:channelId_for_client_HyPeHoUsE::varchar as channel_id
    , t.value:channelTitle_for_client_HyPeHoUsE::varchar as channel_title
    , t.value:comments_disabled_for_client_HyPeHoUsE::boolean as comments_disabled
    , t.value:description_for_client_HyPeHoUsE::varchar as description
    --, DATA_CONTENT:dislikes_for_client_HyPeHoUsE::number as dislikes
    , t.value:garbled_1_for_client_HyPeHoUsE::varchar as garbled_1
    , t.value:garbled_2_for_client_HyPeHoUsE::varchar as garbled_2
    , t.value:garbled_3_for_client_HyPeHoUsE::varchar as garbled_3
    , t.value:garbled_4_for_client_HyPeHoUsE::varchar as garbled_4
    , t.value:garbled_5_for_client_HyPeHoUsE::varchar as garbled_5
    , t.value:publishedAt_for_client_HyPeHoUsE::timestamp_tz as published_at
    , t.value:ratings_disabled_for_client_HyPeHoUsE::boolean as ratings_disabled
    , t.value:tags_for_client_HyPeHoUsE::varchar as tags
    , t.value:thumbnail_link_for_client_HyPeHoUsE::varchar as thumbnail_link
    , t.value:title_for_client_HyPeHoUsE::varchar as title
    , to_date(t.value:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM') as trending_date

from {{ source('knowledge_provider_us', 'raw_video_info_tab') }} sr, table(flatten(sr.$1,'data')) t