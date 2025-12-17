SELECT 
    review_id,
    review_date,
    CURRENT_DATE() as today
FROM {{ ref('fct_reviews') }}
WHERE review_date > CURRENT_DATE()