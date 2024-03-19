with video_info as (
    select *
    from {{ ref('stg_video_info') }}
),

final as (
    select channel_title
    , count(*) as number_of_trending_videos
    from video_info
    group by channel_title
    order by number_of_trending_videos desc
)

select * from final