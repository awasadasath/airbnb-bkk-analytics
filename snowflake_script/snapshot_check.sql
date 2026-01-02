SELECT 
    listing_id, 
    listing_name,
    price, 
    host_is_superhost,
    dbt_valid_from,   -- วันที่เริ่มมีผล
    dbt_valid_to      -- วันที่สิ้นสุดผล (ถ้าเป็น NULL แปลว่าเป็นข้อมูลปัจจุบัน)
FROM airbnb_bkk.snapshots.snp_listings
LIMIT 100;
