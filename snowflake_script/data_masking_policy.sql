-- 1. ใช้ Role ใหญ่สุด เพื่อไม่ให้ติดเรื่อง Permission
USE ROLE ACCOUNTADMIN;

-- 2. เลือก Database
USE DATABASE AIRBNB_BKK;

-- 3. สร้าง Schema 'ANALYTICS' 
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- 4. สร้าง Policy ลงไปใน AIRBNB_BKK.ANALYTICS
CREATE OR REPLACE MASKING POLICY AIRBNB_BKK.ANALYTICS.HOST_MASK AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('ANALYST') THEN '***MASKED***'
    ELSE val
  END;

-- 5. ให้สิทธิ์
-- ให้สิทธิ์กับ Role หลักๆ เพื่อให้ dbt Cloud หยิบไปใช้ได้
GRANT APPLY ON MASKING POLICY AIRBNB_BKK.ANALYTICS.HOST_MASK TO ROLE ACCOUNTADMIN;
GRANT APPLY ON MASKING POLICY AIRBNB_BKK.ANALYTICS.HOST_MASK TO ROLE SYSADMIN; 





-- Test ว่าได้ผลหรือยัง

USE SCHEMA dbt_awasadasath;

CREATE OR REPLACE MASKING POLICY host_mask AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('ANALYST') THEN '***MASKED***'
    ELSE val
  END;
-- เปลี่ยน Role กลับเป็น ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- ลอง Select ดูข้อมูลอีกครั้ง
SELECT listing_id, host_name 
FROM dim_listings 
LIMIT 5;

-- ช่อง host_name ต้องเป็นชื่อคนปกติถ้าเป็น ACCOUNT ADMIN
