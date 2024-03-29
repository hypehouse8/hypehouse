with
video_all as (
    select 
        country_name
        , category_title
        , video_popular_grade
        , view_count
    from {{ ref('video_all') }}
),

avg_popularity_grade_country_category as (
    select
        country_name
        , category_title
        , AVG(video_popular_grade) as avg_popularity_grade
        , SUM(view_count) as avg_view_count
    from video_all
    group by country_name, category_title 
),

final as (
    select
        country_name
        , MAX_BY(category_title, avg_popularity_grade) as the_most_popular_category
        , MAX_BY(category_title, avg_view_count) as the_most_viewed_category
    from avg_popularity_grade_country_category
    group by country_name
)

select * from final