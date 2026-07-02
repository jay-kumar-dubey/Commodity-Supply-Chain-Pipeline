{{ config(materialized='table') }}

SELECT 
    price_date,
    AVG(price_usd) as price_usd
FROM (
    SELECT 
        CAST(record.period as date) as price_date,
        CAST(record.value as float) as price_usd
    FROM (
        SELECT UNNEST(response.data) as record
        FROM read_json_auto('../data/bronze/eia_oil_prices/*.json')
    )
    WHERE CAST(record.value as float) IS NOT NULL
    AND CAST(record.period as date) IS NOT NULL
)
GROUP BY price_date
ORDER BY price_date DESC