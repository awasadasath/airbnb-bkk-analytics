-- Room Type Distribution
SELECT 
    ROOM_TYPE,
    COUNT(*) as Total
FROM AIRBNB_BKK.DBT_AWASADASATH_CORE.DIM_LISTINGS
GROUP BY 1;

-- Avg Price by Room Type
SELECT 
    ROOM_TYPE,
    ROUND(AVG(PRICE), 0) as Avg_Price
FROM AIRBNB_BKK.DBT_AWASADASATH_CORE.DIM_LISTINGS
GROUP BY 1
ORDER BY 2 DESC;

-- Price Statistics by Room Type
SELECT 
    ROOM_TYPE,  
    COUNT(*) as "Total Units",    
    MIN(PRICE) as "Min Price (฿)",  
    MAX(PRICE) as "Max Price (฿)",   
    ROUND(AVG(PRICE), 0) as "Avg Price ($)"
FROM AIRBNB_BKK.DBT_AWASADASATH_CORE.DIM_LISTINGS
GROUP BY 1
ORDER BY 5 DESC;
