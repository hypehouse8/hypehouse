with video_info_all as (
    select
        country_name
        , likes
        , comment_count
        , view_count
        , title
    from {{ ref('video_all') }}
),
final as (
    select
        country_name
        , AVG(likes) as avg_likes
        , AVG(comment_count) as avg_comment_count
        , AVG(view_count) as avg_comment_count
        , MAX(title, view_count) as most_viewed_video
    from video_info_all
    group by country_name
)

select * from final