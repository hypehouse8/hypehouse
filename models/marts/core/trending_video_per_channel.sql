with video_info_all as (
    select 
        channel_title
        , country_name
        , video_id
    from {{ ref('video_all') }}
),

final as (
    select 
        country_name
        , channel_title
        , count(*) as number_of_trending_videos
    from video_info_all
    group by country_name, channel_title
    order by country_name
)

select * from final