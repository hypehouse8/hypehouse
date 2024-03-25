with
    video_info as (select * from {{ ref("stg_video_info") }}),

    --comments as (select * from {{ ref("stg_comment_count") }}),

    --likes as (select * from {{ ref("stg_likes_count") }}),

    views as (select * from {{ ref("stg_view_count") }}),

    --categories as (select * from {{ ref("video_categories") }}),

    countries as (select * from {{ ref('countries') }}),

    final as (
        select
            country_name,
            sum(view_count) as summary_views
        -- , SUM(likes) AS summary_likes
        -- , SUM(comment_count) AS summary_counts
        from countries
        inner join 
            video_info 
            on video_info.country_code = countries.country_code
        left join
            views
            on video_info.video_id = views.video_id AND video_info.date_file = views.date_file AND video_info.country_code = views.country_code
        group by COUNTRY_NAME
        order by summary_views desc
    )

select *
from final
limit 10
