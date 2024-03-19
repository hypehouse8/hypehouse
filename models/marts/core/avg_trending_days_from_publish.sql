with video_info as (
    select *
    from {{ ref('stg_video_info') }}
),

video_trend_time as (
    select *
    , DATEDIFF(day, published_at, trending_date) as days_from_publish_to_trend
    from video_info
)

select AVG(days_from_publish_to_trend) as avg_days_from_published_to_trend from video_trend_time