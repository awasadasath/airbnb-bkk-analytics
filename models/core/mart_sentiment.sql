{{ config(
    materialized = 'table'
) }}

WITH reviews AS (
    SELECT 
        review_id,
        listing_id,
        review_date,
        review_text 
    FROM {{ ref('stg_reviews') }} 
    WHERE review_text IS NOT NULL
    ORDER BY review_date DESC
    LIMIT 100 
)

SELECT
    review_id,
    listing_id,
    review_date,
    review_text,
    SNOWFLAKE.CORTEX.SENTIMENT(
        SNOWFLAKE.CORTEX.TRANSLATE(review_text, '', 'en')
    ) AS sentiment_score
FROM reviews