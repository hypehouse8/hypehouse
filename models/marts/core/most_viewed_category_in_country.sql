with
    video_info as (select * from {{ ref("stg_video_info") }}),

    views as (select * from {{ ref("stg_view_count") }}),

    categories as (select * from {{ ref("video_categories") }}),

    countries as (select * from {{ ref('countries') }}),

    final as (
        select
            country_name,
            category_title,
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
        left join 
            categories
            on video_info.category_id = categories.category_id
        group by country_name, category_title
        order by country_name, summary_views desc  
    )

select country_name, MAX_BY(category_title, summary_views) as most_viewed_category
from final
group by country_name
limit 10