WITH video_info as (
    SELECT 
        date_file
        , video_id
        , category_id
        , channel_id
        , channel_title
        , comments_disabled
        , description
        , garbled_1
        , garbled_2
        , garbled_3
        , garbled_4
        , garbled_5
        , published_at
        , ratings_disabled
        , tags
        , thumbnail_link
        , title
        , trending_date
    FROM {{ ref('stg_video_info') }}
),
comments as (
    SELECT
        date_file
        , comment_count
        , video_id
    FROM {{ ref('stg_comment_count') }}
), 
likes as (
    SELECT
        date_file
        , likes
        , video_id
    FROM {{ ref('stg_likes_count') }}
),
views as (
    SELECT
        date_file
        , view_count
        , video_id
    FROM {{ ref('stg_view_count') }}
),
all_info as (
    SELECT 
        video_info.date_file
        , video_info.video_id
        , category_id
        , channel_id
        , channel_title
        , comments_disabled
        , description
        , garbled_1
        , garbled_2
        , garbled_3
        , garbled_4
        , garbled_5
        , published_at
        , ratings_disabled
        , tags
        , thumbnail_link
        , title
        , trending_date
        , comment_count
        , likes
        , view_count
    FROM video_info
    LEFT JOIN comments ON video_info.video_id = comments.video_id AND video_info.date_file = comments.date_file
    LEFT JOIN likes ON video_info.video_id = likes.video_id AND video_info.date_file = likes.date_file
    LEFT JOIN views ON video_info.video_id = views.video_id AND video_info.date_file = views.date_file
)

SELECT * FROM all_info