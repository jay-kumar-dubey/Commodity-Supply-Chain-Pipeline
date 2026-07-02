{{ config(materialized='table') }}
WITH monthly_oil AS (
    SELECT 
        DATE_TRUNC('month', price_date) as month,
        AVG(price_usd) as avg_oil_price
    FROM {{ ref('silver_eia_oil_prices') }}
    GROUP BY 1
)
, monthly_shipping AS (
    SELECT 
        DATE_TRUNC('month', index_date) as month,
        AVG(index_value) as shipping_index
    FROM {{ ref('silver_shipping_index') }}
    GROUP BY 1
)

, with_lag AS (
    SELECT
        o.month,
        o.avg_oil_price,
        s.shipping_index,
        LAG(o.avg_oil_price) OVER (ORDER BY o.month) as prev_oil,
        LAG(s.shipping_index) OVER (ORDER BY o.month) as prev_shipping
    FROM monthly_oil o
    JOIN monthly_shipping s ON o.month = s.month
)

SELECT
    month,
    avg_oil_price,
    shipping_index,
    (avg_oil_price - prev_oil) / prev_oil * 100 as oil_mom_change_pct,
    (shipping_index - prev_shipping) / prev_shipping * 100 as shipping_mom_change_pct,
    ((shipping_index - prev_shipping) / prev_shipping * 100) - 
    ((avg_oil_price - prev_oil) / prev_oil * 100) as stress_signal
    
FROM with_lag
WHERE prev_oil IS NOT NULL
ORDER BY month DESC