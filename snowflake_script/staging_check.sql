USE SCHEMA dbt_awasadasath;

-- 1. ดู Listings
SELECT * FROM stg_listings LIMIT 50;
-- ดู schema
DESC TABLE dbt_awasadasath.stg_listings;

-- 2. ดู Reviews
SELECT * FROM stg_reviews LIMIT 50;
DESC VIEW DBT_AWASADASATH.stg_reviews;

-- 3. ดู Calendar
SELECT * FROM stg_calendar LIMIT 50;
-- ดู neihborhoods
SELECT * FROM neighborhoods LIMIT 50;
-- เช็ค Price และ Adjusted Price
SELECT 
    listing_id,
    date,
    price,
    adjusted_price
FROM dbt_awasadasath.stg_calendar  
WHERE price != adjusted_price
LIMIT 100;

-- Duplicate Check
SELECT 
    id AS listing_id, 
    COUNT(*) AS duplicate_count
FROM AIRBNB_BKK.RAW.LISTINGS
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


SELECT 
    id AS review_id, 
    COUNT(*) AS duplicate_count
FROM AIRBNB_BKK.RAW.REVIEWS
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

SELECT 
    listing_id, 
    date, 
    COUNT(*) AS duplicate_count
FROM AIRBNB_BKK.RAW.CALENDAR
GROUP BY listing_id, date
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- NULL CHECK

SELECT 
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(review_id) AS null_review_ids,
    COUNT(*) - COUNT(review_date) AS null_review_dates,
    COUNT(*) - COUNT(reviewer_name) AS null_reviewer_names,
    COUNT(*) - COUNT(review_text) AS null_review_texts,
    COUNT(*) - COUNT(listing_id) AS null_listing_ids

FROM dbt_awasadasath.stg_reviews;

SELECT 
    COUNT(*) AS total_rows,
    
    COUNT(*) - COUNT(listing_id) AS null_listing_id,
    COUNT(*) - COUNT(date) AS null_date,
    COUNT(*) - COUNT(is_available) AS null_is_available,
    
    COUNT(*) - COUNT(price) AS null_price,
    COUNT(*) - COUNT(adjusted_price) AS null_adj_price,
    
    COUNT(*) - COUNT(minimum_nights) AS null_min_nights,
    COUNT(*) - COUNT(maximum_nights) AS null_max_nights

FROM dbt_awasadasath.stg_calendar;


SELECT 
    COUNT(*) AS total_rows,

    -- Identity & URLs
    COUNT(*) - COUNT(listing_id) AS null_listing_id,
    COUNT(*) - COUNT(listing_url) AS null_listing_url,
    COUNT(*) - COUNT(listing_name) AS null_listing_name,
    COUNT(*) - COUNT(description) AS null_description,
    COUNT(*) - COUNT(neighborhood_overview) AS null_neighborhood_overview,
    COUNT(*) - COUNT(picture_url) AS null_picture_url,

    -- Property Details
    COUNT(*) - COUNT(property_type) AS null_property_type,
    COUNT(*) - COUNT(room_type) AS null_room_type,
    COUNT(*) - COUNT(accommodates) AS null_accommodates,
    COUNT(*) - COUNT(bedrooms) AS null_bedrooms,
    COUNT(*) - COUNT(beds) AS null_beds,
    COUNT(*) - COUNT(bathrooms_text) AS null_bathrooms_text,
    COUNT(*) - COUNT(amenities) AS null_amenities,

    -- Price
    COUNT(*) - COUNT(price) AS null_price,

    -- Booking Rules
    COUNT(*) - COUNT(minimum_nights) AS null_minimum_nights,
    COUNT(*) - COUNT(maximum_nights) AS null_maximum_nights,
    COUNT(*) - COUNT(instant_bookable) AS null_instant_bookable,

    -- Availability
    COUNT(*) - COUNT(has_availability) AS null_has_availability,
    COUNT(*) - COUNT(availability_30) AS null_availability_30,
    COUNT(*) - COUNT(availability_60) AS null_availability_60,
    COUNT(*) - COUNT(availability_90) AS null_availability_90,
    COUNT(*) - COUNT(availability_365) AS null_availability_365,

    -- Host Info
    COUNT(*) - COUNT(host_id) AS null_host_id,
    COUNT(*) - COUNT(host_url) AS null_host_url,
    COUNT(*) - COUNT(host_name) AS null_host_name,
    COUNT(*) - COUNT(host_since) AS null_host_since,
    COUNT(*) - COUNT(host_location) AS null_host_location,
    COUNT(*) - COUNT(host_about) AS null_host_about,
    COUNT(*) - COUNT(host_response_time) AS null_host_response_time,
    COUNT(*) - COUNT(host_response_rate) AS null_host_response_rate,
    COUNT(*) - COUNT(host_acceptance_rate) AS null_host_acceptance_rate,
    COUNT(*) - COUNT(host_thumbnail_url) AS null_host_thumbnail_url,
    COUNT(*) - COUNT(host_picture_url) AS null_host_picture_url,
    COUNT(*) - COUNT(host_neighbourhood) AS null_host_neighbourhood,
    COUNT(*) - COUNT(host_verifications) AS null_host_verifications,

    -- Host Metrics
    COUNT(*) - COUNT(calculated_host_listings_count) AS null_calc_host_count,
    COUNT(*) - COUNT(calculated_host_listings_count_entire_homes) AS null_calc_host_entire,
    COUNT(*) - COUNT(calculated_host_listings_count_private_rooms) AS null_calc_host_private,
    COUNT(*) - COUNT(calculated_host_listings_count_shared_rooms) AS null_calc_host_shared,

    -- Host Status Boolean
    COUNT(*) - COUNT(host_is_superhost) AS null_host_is_superhost,
    COUNT(*) - COUNT(host_identity_verified) AS null_host_identity_verified,
    COUNT(*) - COUNT(host_has_profile_pic) AS null_host_has_profile_pic,

    -- Location
    COUNT(*) - COUNT(neighbourhood_name) AS null_neighbourhood_name,
    COUNT(*) - COUNT(latitude) AS null_latitude,
    COUNT(*) - COUNT(longitude) AS null_longitude,

    -- Reviews Stats
    COUNT(*) - COUNT(number_of_reviews) AS null_number_of_reviews,
    COUNT(*) - COUNT(number_of_reviews_ltm) AS null_number_of_reviews_ltm,
    COUNT(*) - COUNT(number_of_reviews_l30d) AS null_number_of_reviews_l30d,
    COUNT(*) - COUNT(reviews_per_month) AS null_reviews_per_month,

    -- Review Dates
    COUNT(*) - COUNT(first_review) AS null_first_review,
    COUNT(*) - COUNT(last_review) AS null_last_review,

    -- Review Scores
    COUNT(*) - COUNT(review_scores_rating) AS null_review_scores_rating,
    COUNT(*) - COUNT(review_scores_accuracy) AS null_review_scores_accuracy,
    COUNT(*) - COUNT(review_scores_cleanliness) AS null_review_scores_cleanliness,
    COUNT(*) - COUNT(review_scores_checkin) AS null_review_scores_checkin,
    COUNT(*) - COUNT(review_scores_communication) AS null_review_scores_communication,
    COUNT(*) - COUNT(review_scores_location) AS null_review_scores_location,
    COUNT(*) - COUNT(review_scores_value) AS null_review_scores_value

FROM dbt_awasadasath.stg_listings;
