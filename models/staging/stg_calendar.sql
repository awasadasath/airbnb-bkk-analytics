WITH source AS (
    SELECT * FROM {{ source('airbnb', 'calendar') }}
),

listings AS (
    SELECT listing_id, price AS listing_price
    FROM {{ ref('stg_listings') }}
),

renamed AS (
    SELECT
        source.listing_id,
        
        -- แปลง Text เป็น Date
        CAST(source.date AS DATE) AS date,
        
        -- Logic ราคา: ใช้ราคาจากปฏิทิน ถ้าไม่มีให้ใช้ราคามาตรฐานจาก listing
        COALESCE(
            {{ clean_price('source.price') }}, 
            listings.listing_price
        ) AS price,
        
        -- แปลงเป็นจำนวนเต็ม (Integer)
        CAST(source.minimum_nights AS INT) AS minimum_nights,
        CAST(source.maximum_nights AS INT) AS maximum_nights,
        
        -- แปลง Availability เป็น Boolean
        CASE WHEN source.available = 't' THEN TRUE ELSE FALSE END AS is_available

    FROM source
    -- กรองเอาเฉพาะห้องที่มีอยู่จริงในตาราง Listings
    INNER JOIN listings ON source.listing_id = listings.listing_id
)

SELECT * FROM renamed
-- กรองราคาที่เป็น NULL ทิ้ง (กรณีที่หาไม่ได้ทั้งสองทาง)
WHERE price IS NOT NULL