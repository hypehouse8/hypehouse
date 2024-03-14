select
    sr.date_file as date_file
    , t.value:view_count_for_client_HyPeHoUsE::number as view_count
    , t.value:video_id_for_client_HyPeHoUsE::varchar as video_id
    
    --, DATA_CONTENT:index::int as index_view

FROM {{ source('knowledge_provider_us', 'raw_view_count_tab') }} sr, table(flatten(sr.$1,'data')) t