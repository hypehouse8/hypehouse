with
    video_info as (select * from {{ ref("stg_video_info") }}),

    comments as (select * from {{ ref("stg_comment_count") }}),

    likes as (select * from {{ ref("stg_likes_count") }}),

    views as (select * from {{ ref("stg_view_count") }}),

    categories as (select * from {{ ref("video_categories") }}),

    final as (
        select
            -- video_info.category_id as category_id
            categories.category_title as category_title,
            -- , channel_id
            -- , channel_title
            -- , title
            -- , trending_date
            -- , category_id
            sum(view_count) as summary_views
        -- , SUM(likes) AS summary_likes
        -- , SUM(comment_count) AS summary_counts
        from categories
        inner join 
            video_info 
            on video_info.category_id = categories.category_id
        --left join
        --    comments
        --   on video_info.video_id = comments.video_id
        --    and video_info.date_file = comments.date_file
        --left join
        --    likes
        --   on video_info.video_id = likes.video_id
        --    and video_info.date_file = likes.date_file
        left join
            views
            on video_info.video_id = views.video_id AND video_info.date_file = views.date_file AND video_info.country_code = views.country_code
        group by category_title
        order by summary_views desc
    )

select *
from final
limit 10
