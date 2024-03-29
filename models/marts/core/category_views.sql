with video_info as (
    select *
    from {{ ref('stg_video_info') }}
),

views as (
    select *
    from {{ ref('stg_view_count') }}
),

video_views as (
    select video_info.category_id
    , views.view_count as view_count
    from video_info
    inner join views 
    on video_info.video_id = views.video_id and video_info.date_file = views.date_file and video_info.country_code = views.country_code
),

categories as (
    select * from {{ ref('video_categories') }}
),
catogories_views as (
    select COALESCE(category_title, 'Unknown') as category_title
    , AVG(view_count) as average_views
    from video_views
    left join categories on video_views.category_id = categories.category_id
    group by category_title
    ORDER BY average_views DESC
)

select * from catogories_views
