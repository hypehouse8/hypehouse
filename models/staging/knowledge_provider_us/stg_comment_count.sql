select
    TO_DATE(SUBSTRING(sr.file_name,3,8),'YY.DD.MM') as date_file
    , t.value:comment_count_for_client_HyPeHoUsE::number as comment_count
    , t.value:video_id_for_client_HyPeHoUsE::varchar as video_id
    , SUBSTRING(sr.file_name,12,2) as country_code
    --, DATA_CONTENT:index::int as index_comment
from {{ source('knowledge_provider_us', 'raw_comment_count_tab') }} sr, table(flatten(sr.$1,'data')) t