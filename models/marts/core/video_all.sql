WITH video_info as (
    SELECT 
        date_file
        , video_id
        , category_id
        , channel_id
        , channel_title
        , comments_disabled
        , description
        , published_at
        , ratings_disabled
        , tags
        , thumbnail_link
        , title
        , trending_date
        , country_code
    FROM {{ ref('stg_video_info') }}
),
comments as (
    SELECT
        date_file
        , comment_count
        , video_id
        , country_code
    FROM {{ ref('stg_comment_count') }}
), 
likes as (
    SELECT
        date_file
        , likes
        , video_id
        , country_code
    FROM {{ ref('stg_likes_count') }}
),
views as (
    SELECT
        date_file
        , view_count
        , video_id
        , country_code
    FROM {{ ref('stg_view_count') }}
),

countries as (
    select
        country_code
        , country_name
    from {{ ref('countries') }}
),

categories as (
    select 
        category_id
        , category_title
    from {{ ref('video_categories') }}
),

all_info as (
    SELECT 
        video_info.date_file
        , video_info.video_id
        , categories.category_title
        , channel_id
        , channel_title
        , comments_disabled
        , description
        , published_at
        , ratings_disabled
        , tags
        , thumbnail_link
        , title
        , trending_date
        , comment_count
        , likes
        , view_count
        , countries.country_name
    FROM video_info
    LEFT JOIN comments 
        ON video_info.video_id = comments.video_id 
        AND video_info.date_file = comments.date_file 
        AND video_info.country_code = comments.country_code
    LEFT JOIN likes 
        ON video_info.video_id = likes.video_id 
        AND video_info.date_file = likes.date_file 
        AND video_info.country_code = likes.country_code
    LEFT JOIN views 
        ON video_info.video_id = views.video_id 
        AND video_info.date_file = views.date_file 
        AND video_info.country_code = views.country_code
    LEFT JOIN countries
        ON video_info.country_code = countries.country_code
    left join categories
        on video_info.category_id = categories.category_id
)

SELECT * FROM all_info