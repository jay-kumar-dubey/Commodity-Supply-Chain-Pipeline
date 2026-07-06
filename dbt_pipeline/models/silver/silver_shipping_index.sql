{{ config(
    materialized='incremental',
    unique_key='index_date',
    post_hook="{{ export_to_s3('silver_shipping_index', 'silver/baltic_dry_index') }}"
) }}

SELECT 
    index_date,
    AVG(index_value) as index_value
FROM (
    SELECT 
        CAST(record.date as date) as index_date,
        CAST(record.value as float) as index_value
    FROM (
        SELECT unnest(observations) as record
        FROM read_json_auto('../data/bronze/baltic_dry_index/*.json')
    )
    WHERE CAST(record.value as float) IS NOT NULL
    AND CAST(record.date as date) IS NOT NULL
)
GROUP BY index_date
{% if is_incremental() %}
HAVING index_date > (SELECT MAX(index_date) FROM {{ this }})
{% endif %}
ORDER BY index_date DESC