{{ config(
    materialized='table',
    schema='core'
) }}

WITH reviews AS (
    -- 1. ดึงข้อมูลรีวิวที่ clean แล้วจาก Staging
    SELECT 
        review_id, 
        listing_id, 
        review_date,
        reviewer_id,
        reviewer_name, 
        review_text 
    FROM {{ ref('stg_reviews') }}
),

listings AS (
    -- 2. ดึงข้อมูล Dimension จาก Listings
    SELECT 
        listing_id, 
        host_id,
        host_since,
        neighbourhood_name
    FROM {{ ref('stg_listings') }}
)

SELECT
    -- Keys & IDs
    r.review_id,
    r.listing_id,
    r.reviewer_id,
    l.host_id, 

    -- Date & Time
    r.review_date,
    
    -- Metrics
    -- คำนวณอายุของรีวิว: กี่วันจากวันนี้
    DATEDIFF('day', r.review_date, CURRENT_DATE()) AS review_age_days,
    
    -- Dimension Attributes
    r.reviewer_name,
    l.neighbourhood_name,
    
    -- Text
    r.review_text
    
FROM reviews r
INNER JOIN listings l
    ON r.listing_id = l.listing_id
-- กรองแถวที่ไม่จำเป็นออก
WHERE r.review_text IS NOT NULL 
  AND r.review_date IS NOT NULL