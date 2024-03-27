
with video_info as (
    select *
    from {{ ref('stg_video_info') }}
),

countries as (
    select *
    from {{ ref('countries') }}
),

video_trend_time as (
    select countries.country_name
    , AVG(DATEDIFF(day, published_at, trending_date)) as avg_days_from_published_to_trend
    from video_info
    join countries on video_info.country_code = countries.country_code
    group by countries.country_name
    order by avg_days_from_published_to_trend
)

select * from video_trend_time