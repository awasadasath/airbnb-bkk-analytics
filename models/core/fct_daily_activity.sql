{{ config(
    materialized='table',
    schema='core'
) }}

WITH calendar AS (
    -- 1. ดึงข้อมูลปฏิทินที่ Clean แล้ว (มี Price และ Availability รายวัน)
    SELECT
        listing_id,
        date,
        price,
        is_available,
        minimum_nights,
        maximum_nights
    FROM {{ ref('stg_calendar') }}
),

listings AS (
    -- 2. ดึงข้อมูล Host ID และ Location จาก Listings Dimension
    SELECT
        listing_id,
        host_id,
        neighbourhood_name
    FROM {{ ref('dim_listings') }}
)

SELECT
    -- Keys & IDs
    c.listing_id,
    l.host_id,
    
    -- Date Dimension
    c.date AS activity_date,
    
    -- Location Dimension Attribute
    l.neighbourhood_name,
    
    -- Metrics
    c.price,
    c.is_available,
    
    -- Rules
    c.minimum_nights,
    c.maximum_nights
    
FROM calendar c
-- Join กับ Dimension Listings เพื่อนำ Host ID และ Location มาใช้
INNER JOIN listings l
    ON c.listing_id = l.listing_id

-- กรองข้อมูลที่ไม่สมบูรณ์ (ราคาต้องไม่เป็น NULL และต้องมีวันที่)
WHERE c.price IS NOT NULL
  AND c.date IS NOT NULL