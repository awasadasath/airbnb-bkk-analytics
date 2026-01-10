{{ config(
    materialized = 'table'
) }}

WITH source_listings AS (
    SELECT 
        listing_id,
        listing_name,
        neighbourhood_name,
        price,
        description
    FROM {{ ref('dim_listings') }}
    WHERE description IS NOT NULL
)

SELECT
    listing_id,
    listing_name,
    neighbourhood_name,
    price,
    
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-7b',
        CONCAT(
            'Classify this Airbnb description into exactly one of these 4 categories: ',
            '"Luxury", "Local Vibe", "Modern", "Budget". ',
            'Return ONLY the category
             name without explanation. ',
            'Description: ', SUBSTR(description, 1, 1000)
        )
    ) AS listing_vibe

FROM source_listings
LIMIT 100