with video_info_all as (
    select
        country_name
        , likes
        , comment_count
        , view_count
        , video_popular_grade
        , channel_title
        , title
        , published_at
        , trending_date
    from {{ ref('video_all') }}
),
final as (
    select
        country_name
        , round(AVG(likes),2) as avg_likes
        , round(AVG(comment_count),2) as avg_comment_count
        , round(AVG(view_count),2) as avg_view_count
        , round(AVG(video_popular_grade),2) as avg_popularity_grade
        , MAX_BY(channel_title, video_popular_grade) as most_popular_channel
        , MAX_BY(title, video_popular_grade) as most_popular_video
        , AVG(DATEDIFF(day, published_at, trending_date)) as avg_days_from_published_to_trend
    from video_info_all
    group by country_name
)

select * from final