SELECT COUNT(*) FROM DBT_AWASADASATH_CORE.dim_listings;

-- สร้างสถานการณ์จำลอง: เผลอลบข้อมูลทิ้งหมดเลย
DELETE FROM DBT_AWASADASATH_CORE.dim_listings;

-- ตรวจดูความเสียหาย (เหลือ 0 แถว)
SELECT COUNT(*) FROM DBT_AWASADASATH_CORE.dim_listings;

-- ใช้ Time Travel ดูข้อมูลเมื่อ 2 นาทีก่อน
-- (OFFSET -60*2 หมายถึง ย้อนหลังไป 120 วินาที)
SELECT COUNT(*) 
FROM DBT_AWASADASATH_CORE.dim_listings AT(OFFSET => -60*2);

-- 5. กู้ข้อมูลกลับคืนมา
INSERT INTO DBT_AWASADASATH_CORE.dim_listings
SELECT * FROM DBT_AWASADASATH_CORE.dim_listings AT(OFFSET => -60*2);

-- 6. ตรวจสอบอีกครั้ง
SELECT COUNT(*) FROM DBT_AWASADASATH_CORE.dim_listings;
