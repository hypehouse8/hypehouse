select
    TO_DATE(SUBSTRING(sr.file_name,3,8),'YY.DD.MM') as date_file
    , t.value:video_id_for_client_HyPeHoUsE::varchar as video_id
    --, DATA_CONTENT:index::int as index_video
    , t.value:categoryId_for_client_HyPeHoUsE::number as category_id
    , t.value:channelId_for_client_HyPeHoUsE::varchar as channel_id
    , t.value:channelTitle_for_client_HyPeHoUsE::varchar as channel_title
    , t.value:comments_disabled_for_client_HyPeHoUsE::boolean as comments_disabled
    , t.value:description_for_client_HyPeHoUsE::varchar as description
    --, DATA_CONTENT:dislikes_for_client_HyPeHoUsE::number as dislikes
    , t.value:publishedAt_for_client_HyPeHoUsE::timestamp_tz as published_at
    , t.value:ratings_disabled_for_client_HyPeHoUsE::boolean as ratings_disabled
    , t.value:tags_for_client_HyPeHoUsE::varchar as tags
    , t.value:thumbnail_link_for_client_HyPeHoUsE::varchar as thumbnail_link
    , t.value:title_for_client_HyPeHoUsE::varchar as title
    , to_date(t.value:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM') as trending_date
    , SUBSTRING(sr.file_name,12,2) as country_code

from {{ source('hypehouse', 'video_info_raw') }} sr, table(flatten(sr.$1,'data')) t