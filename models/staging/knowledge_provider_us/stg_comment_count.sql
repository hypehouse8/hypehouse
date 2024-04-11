with source as(

    SELECT *, _FILE_NAME as filename FROM {{ source('hypehouse', 'comment_count_raw') }}

),
final as (
SELECT PARSE_DATE('%y.%d.%m',SUBSTRING(filename,17,8)) as date
    , SUBSTRING(filename,26,2) as country_code
    , data.comment_count_for_client_HyPeHoUsE as comment_count
    , data.video_id_for_client_HyPeHoUsE as video_id
    
FROM source, UNNEST(data) as data
)
select * from final