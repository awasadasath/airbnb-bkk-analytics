{{ config(
    materialized='table', 
    schema='aggregate'
) }}

SELECT
    activity_date,
    neighbourhood_name,
    
    -- 1. Performance Metrics
    COUNT(listing_id) AS total_listings_count,
    AVG(price) AS average_daily_price,
    
    -- 2. Occupancy Metrics
    SUM(CASE WHEN is_available = FALSE THEN 1 ELSE 0 END) AS booked_listings_count,
    SUM(CASE WHEN is_available = TRUE THEN 1 ELSE 0 END) AS available_listings_count,
    
    -- 3. Calculate Occupancy Rate
    -- (จำนวนห้องที่ไม่ว่าง / จำนวนห้องทั้งหมดในย่านนั้นและวันนั้น)
    (CAST(SUM(CASE WHEN is_available = FALSE THEN 1 ELSE 0 END) AS REAL) * 100) 
        / COUNT(listing_id) AS occupancy_rate_percent
        
FROM {{ ref('fct_daily_activity') }}
GROUP BY 1, 2
ORDER BY 1, 2