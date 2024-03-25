{{
    config(
        materialized = 'incremental',
        unique_key = 'id'
    )
}}
WITH source_data AS (
    SELECT * FROM {{ ref('source_seed') }}
)
SELECT
    *,
    '{{ invocation_id}}' as batch_id
FROM
    source_data
WHERE
    -- This condition allows incremental models to only process new/updated records
    {% if is_incremental() %}
        -- Adjust the timestamp column and comparison for your use case
        invoiced_at > (SELECT MAX(invoiced_at) FROM {{ this }})
    {% else %}
        1=1
    {% endif %}