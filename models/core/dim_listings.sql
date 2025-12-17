{{
  config(
    materialized = 'table',
    schema='core',
    post_hook = "ALTER TABLE {{ this }} MODIFY COLUMN host_name SET MASKING POLICY {{ target.schema }}.host_mask"
  )
}}

SELECT
    -- Primary Key
    listing_id,
    
    -- Listing Details
    listing_name,
    description,
    neighborhood_overview,
    picture_url,
    property_type,
    room_type,
    
    -- Capacity & Size
    accommodates,
    bedrooms,
    beds,
    
    -- Bathroom Details
    bathrooms,
    bathroom_type,
    
    -- Price (ใช้ราคาฐาน)
    price,
    minimum_nights,
    maximum_nights,
    instant_bookable,
    
    -- Location
    neighbourhood_name,
    latitude,
    longitude,
    
    -- Review Scores
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    
    -- Host Details (Host Dimension Attribute)
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_identity_verified,
    host_response_rate,
    host_acceptance_rate,
    
    --  Host Capacity
    calculated_host_listings_count AS host_total_listings
    
FROM {{ ref('stg_listings') }}

ORDER BY neighbourhood_name, host_is_superhost DESC