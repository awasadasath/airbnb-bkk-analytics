-- 1. เช็คค่าเดิมก่อน
SELECT id, price, host_is_superhost 
FROM AIRBNB_BKK.RAW.LISTINGS 
WHERE id = '27934'; 

-- 2. แกล้งแก้ข้อมูลในตาราง RAW
UPDATE AIRBNB_BKK.RAW.LISTINGS
SET 
    price = '$9,999.00',         
    host_is_superhost = 't'      
WHERE id = '27934';


SELECT 
    listing_id, 
    price, 
    host_is_superhost, 
    dbt_valid_from, 
    dbt_valid_to,
    dbt_scd_id
FROM AIRBNB_BKK.SNAPSHOTS.SNP_LISTINGS
WHERE listing_id = '27934'
ORDER BY dbt_valid_from;
