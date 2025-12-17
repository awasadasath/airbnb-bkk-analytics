WITH source AS (
    SELECT * FROM {{ source('airbnb', 'reviews') }}
),

renamed AS (
    SELECT
        listing_id,
        id AS review_id,
        
        -- แปลง Text เป็น Date
        CAST(date AS DATE) AS review_date,
        
        reviewer_id,
        
        -- จัดการชื่อคนรีวิว (ถ้าว่างให้เป็น Anonymous)
        COALESCE(reviewer_name, 'Anonymous') AS reviewer_name,
        
        -- จัดการข้อความรีวิว (ถ้าว่างให้บอกว่าไม่มีข้อความ)
        COALESCE(comments, 'No comment') AS review_text

    FROM source
)

SELECT * FROM renamed