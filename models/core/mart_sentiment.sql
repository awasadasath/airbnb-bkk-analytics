{{ config(
    materialized = 'table'
) }}

WITH reviews AS (
    SELECT 
        review_id,
        listing_id,
        review_date,
        review_text
    FROM {{ ref('fct_reviews') }}
    WHERE review_text IS NOT NULL
    AND review_date >= '2025-01-01'
    LIMIT 100
)

SELECT
    review_id,
    listing_id,
    review_date,
    review_text AS original_text,
    
    SNOWFLAKE.CORTEX.TRANSLATE(review_text, '', 'en') AS translated_text,
    
    SNOWFLAKE.CORTEX.SENTIMENT(
        SNOWFLAKE.CORTEX.TRANSLATE(review_text, '', 'en')
    ) AS sentiment_score

FROM reviews