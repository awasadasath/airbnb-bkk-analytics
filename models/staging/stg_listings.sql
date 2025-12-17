WITH source AS (
    SELECT * FROM {{ source('airbnb', 'listings') }}
),

renamed AS (
    SELECT
        -- Identifiers
        id AS listing_id,
        listing_url,
        name AS listing_name,
        COALESCE(description, 'No description provided') AS description,
        neighborhood_overview,
        picture_url,
        
        -- Property Details
        property_type,
        room_type,
        
        CAST(accommodates AS INT) AS accommodates,
        
        -- แก้ทศนิยม: แปลงเป็น INT เพื่อตัด .0000 ออก
        CAST(COALESCE(bedrooms, 0) AS INT) AS bedrooms,
        CAST(COALESCE(beds, 0) AS INT) AS beds,
        
        -- Logic แยกห้องน้ำ
        bathrooms_text, -- เก็บไว้ตรวจสอบความถูกต้อง (ยังไม่ Drop ในชั้นนี้)
        
        CASE 
            WHEN bathrooms_text ILIKE '%half-bath%' THEN 0.5
            WHEN REGEXP_SUBSTR(bathrooms_text, '[0-9]+\\.?[0-9]*') IS NOT NULL 
                 THEN CAST(REGEXP_SUBSTR(bathrooms_text, '[0-9]+\\.?[0-9]*') AS NUMERIC(10,1))
            ELSE 0
        END AS bathrooms,
        
        TRIM(REGEXP_REPLACE(bathrooms_text, '[0-9]+\\.?[0-9]*', '')) AS bathroom_type,
        
        amenities,
        
        -- Price
        {{ clean_price('price') }} AS price,
        
        -- แปลงเป็นตัวเลขจำนวนเต็ม
        CAST(minimum_nights AS INT) AS minimum_nights,
        CAST(maximum_nights AS INT) AS maximum_nights,
        CASE WHEN instant_bookable = 't' THEN TRUE ELSE FALSE END AS instant_bookable,
        
        -- Availability
        CASE WHEN has_availability = 't' THEN TRUE ELSE FALSE END AS has_availability,
        CAST(availability_30 AS INT) AS availability_30,
        CAST(availability_60 AS INT) AS availability_60,
        CAST(availability_90 AS INT) AS availability_90,
        CAST(availability_365 AS INT) AS availability_365,
        
        -- Host Details
        host_id,
        host_url,
        COALESCE(host_name, 'Unknown') AS host_name,
        
        -- แปลงเป็นวันที่
        CAST(host_since AS DATE) AS host_since,
        
        host_location,
        host_about,
        COALESCE(host_response_time, 'N/A') AS host_response_time,
        COALESCE(host_response_rate, 'N/A') AS host_response_rate,
        COALESCE(host_acceptance_rate, 'N/A') AS host_acceptance_rate,
        host_thumbnail_url,
        host_picture_url,
        host_neighbourhood,
        host_verifications,
        
        -- แปลง Host Metrics เป็นตัวเลข
        CAST(calculated_host_listings_count AS INT) AS calculated_host_listings_count,
        CAST(calculated_host_listings_count_entire_homes AS INT) AS calculated_host_listings_count_entire_homes,
        CAST(calculated_host_listings_count_private_rooms AS INT) AS calculated_host_listings_count_private_rooms,
        CAST(calculated_host_listings_count_shared_rooms AS INT) AS calculated_host_listings_count_shared_rooms,
        
        -- Host Status
        CASE WHEN host_is_superhost = 't' THEN TRUE ELSE FALSE END AS host_is_superhost,
        CASE WHEN host_identity_verified = 't' THEN TRUE ELSE FALSE END AS host_identity_verified,
        CASE WHEN host_has_profile_pic = 't' THEN TRUE ELSE FALSE END AS host_has_profile_pic,
        
        -- Location
        neighbourhood_cleansed AS neighbourhood_name,
        -- แปลงเป็นพิกัดทศนิยม
        CAST(latitude AS NUMERIC(18,6)) AS latitude,
        CAST(longitude AS NUMERIC(18,6)) AS longitude,
        
        -- Reviews Stats แปลงเป็นตัวเลข
        CAST(number_of_reviews AS INT) AS number_of_reviews,
        CAST(number_of_reviews_ltm AS INT) AS number_of_reviews_ltm,
        CAST(number_of_reviews_l30d AS INT) AS number_of_reviews_l30d,
        COALESCE(reviews_per_month, 0) AS reviews_per_month,
        
        -- Review Dates แปลงเป็นวันที่
        CAST(first_review AS DATE) AS first_review,
        CAST(last_review AS DATE) AS last_review,
        
        -- Review Scores แปลงเป็นตัวเลขทศนิยม
        CAST(review_scores_rating AS NUMERIC(10,2)) AS review_scores_rating,
        CAST(review_scores_accuracy AS NUMERIC(10,2)) AS review_scores_accuracy,
        CAST(review_scores_cleanliness AS NUMERIC(10,2)) AS review_scores_cleanliness,
        CAST(review_scores_checkin AS NUMERIC(10,2)) AS review_scores_checkin,
        CAST(review_scores_communication AS NUMERIC(10,2)) AS review_scores_communication,
        CAST(review_scores_location AS NUMERIC(10,2)) AS review_scores_location,
        CAST(review_scores_value AS NUMERIC(10,2)) AS review_scores_value

    FROM source
)

SELECT * FROM renamed
WHERE price IS NOT NULL