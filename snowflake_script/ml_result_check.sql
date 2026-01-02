-- XGBoost
-- ดูราคาที่ AI ทำนายเทียบราคาจริง
SELECT * FROM DBT_AWASADASATH.predict_price LIMIT 10;

-- เช็คตาเปล่า
SELECT 
    price,
    ROUND(predicted_price, 0) AS predicted,
    (price - ROUND(predicted_price, 0)) AS diff_money, -- ทายผิดไปกี่บาท (ติดลบ = ทายเวอร์ไป, บวก = ทายถูกไป)
    ROUND(ABS(price - predicted_price) / NULLIF(price, 0) * 100, 2) AS diff_percent -- ผิดไปกี่ %
FROM DBT_AWASADASATH.predict_price
WHERE price < 1000 -- ลองดูเฉพาะห้องราคาปกติก่อน เพราะพวกหลักแสนมันจะเพี้ยนเยอะ
ORDER BY ABS(diff_money) DESC -- ดูตัวที่ผิดเยอะสุดก่อน
LIMIT 20;

-- MAE, MAPE
SELECT
    COUNT(*) as total_rows,
    ROUND(AVG(ABS(PRICE - PREDICTED_PRICE)), 2) as MAE_baht, -- เฉลี่ยแล้วทายผิดกี่บาท
    ROUND(AVG(ABS(PRICE - PREDICTED_PRICE) / NULLIF(PRICE, 0)) * 100, 2) as MAPE_percent -- เฉลี่ยแล้วทายผิดกี่ %
FROM DBT_AWASADASATH.predict_price;

-- หลังตัด Anomaly ต่างๆ
SELECT
    CASE WHEN b.is_anomaly = -1 THEN 'Anomaly' ELSE 'Normal' END as data_type,
    COUNT(*) as count,
    ROUND(AVG(ABS(a.PRICE - a.PREDICTED_PRICE)), 2) as MAE_baht,
    ROUND(AVG(ABS(a.PRICE - a.PREDICTED_PRICE) / NULLIF(a.PRICE, 0)) * 100, 2) as MAPE_percent
FROM DBT_AWASADASATH.predict_price a
JOIN DBT_AWASADASATH.detect_anomalies b USING(listing_id)
GROUP BY 1;



-- Isolation Forest
-- ดูว่าใครคือ Anomaly
SELECT * FROM DBT_AWASADASATH.detect_anomalies LIMIT 10;

SELECT 
    IS_ANOMALY, 
    COUNT(*) as count,
    (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()) as percentage
FROM DBT_AWASADASATH.detect_anomalies -- เปลี่ยน Schema เป็นของคุณ
GROUP BY IS_ANOMALY;

SELECT 
    listing_id,
    price,
    room_type,
    number_of_reviews,
    review_scores_rating,
    is_anomaly
FROM DBT_AWASADASATH.detect_anomalies
JOIN DBT_AWASADASATH_CORE.dim_listings USING(listing_id)
WHERE is_anomaly = -1 -- ขอดูเฉพาะพวกผิดปกติ
ORDER BY price DESC -- ลองเรียงดูตัวแพงๆ ก่อน
LIMIT 20;

SELECT 
    IS_ANOMALY,
    ROUND(AVG(PRICE), 2) as avg_price,
    ROUND(AVG(NUMBER_OF_REVIEWS), 2) as avg_reviews,
    ROUND(AVG(REVIEW_SCORES_RATING), 2) as avg_rating
FROM DBT_AWASADASATH.detect_anomalies
JOIN DBT_AWASADASATH_CORE.dim_listings USING(listing_id)
GROUP BY IS_ANOMALY;


-- K-means Clustering
-- ดูการแบ่งกลุ่มลูกค้า
SELECT * FROM DBT_AWASADASATH.cluster_listings LIMIT 10;

SELECT 
    CLUSTER_LABEL,
    COUNT(*) AS total_listings,
    ROUND(AVG(PRICE), 2) AS avg_price,
    MIN(PRICE) as min_price,
    MAX(PRICE) as max_price,
    MODE(ROOM_TYPE) as most_common_room_type,
    ROUND(AVG(review_scores_rating), 2) as avg_rating,
    ROUND(AVG(accommodates), 1) as avg_capacity
FROM DBT_AWASADASATH.cluster_listings
JOIN DBT_AWASADASATH_CORE.dim_listings USING(listing_id) -- Join กลับไปเอาข้อมูลอื่นมาดูประกอบ
GROUP BY CLUSTER_LABEL
ORDER BY avg_price;
