WITH video_info as (
    SELECT 
        *
    FROM {{ ref('stg_video_info') }}
),
comments as (
    SELECT
       *
    FROM {{ ref('stg_comment_count') }}
), 
likes as (
    SELECT
        *
    FROM {{ ref('stg_likes_count') }}
),
views as (
    SELECT
        *
    FROM {{ ref('stg_view_count') }}
),
final as (
    SELECT 
        video_info.title as video_title
        --, channel_id
        --, channel_title
        --, title
        --, trending_date
        --, category_id
        --, SUM(likes) AS summary_likes
        --, SUM(comment_count) AS summary_counts
        , SUM(view_count) AS summary_views
        
    FROM video_info
    --LEFT JOIN comments ON video_info.video_id = comments.video_id AND video_info.date_file = comments.date_file
    --LEFT JOIN likes ON video_info.video_id = likes.video_id AND video_info.date_file = likes.date_file
    LEFT JOIN views ON video_info.video_id = views.video_id AND video_info.date_file = views.date_file AND video_info.country_code = views.country_code
    GROUP BY video_title 
    ORDER BY summary_views DESC
)

SELECT * FROM final LIMIT 10