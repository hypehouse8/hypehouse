WITH video_info as (
    SELECT *
    FROM {{ ref('stg_video_info') }}
),
comments as (
    SELECT *
    FROM {{ ref('stg_comment_count') }}
), 
all_info as (
    SELECT *
    FROM video_info
    LEFT JOIN comments ON video_info.video_id = comments.video_id AND video_info.date_file = comments.date_file
)

SELECT count(*) FROM all_info