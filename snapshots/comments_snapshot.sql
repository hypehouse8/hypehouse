{% snapshot comments_snapshot %}

{{
    config(
      target_database='dev',
      target_schema='dbt_hh8',
      unique_key='id',

      strategy='timestamp',
      updated_at='date_file',
    )
}}

select video_id||'-'||country_code as id, * from {{ ref('stg_comment_count') }}

{% endsnapshot %}