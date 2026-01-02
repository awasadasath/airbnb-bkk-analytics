-- 1. กำหนดตัวแปร
set airbyte_role = 'AIRBYTE_ROLE';
set airbyte_username = 'AIRBYTE_USER';
set airbyte_warehouse = 'AIRBNB_COMPUTE_WH';  -- ชื่อ Warehouse 
set airbyte_database = 'AIRBNB_BKK';          -- ชื่อ Database 
set airbyte_schema = 'PUBLIC';                -- Default

-- 2. กำหนดรหัสผ่าน
set airbyte_password = '<INSERT_PASSWORD_HERE>';       

begin;

-- ส่วนสร้าง User และ Role (ใช้ securityadmin)
use role securityadmin;

-- สร้าง Role
create role if not exists identifier($airbyte_role);
grant role identifier($airbyte_role) to role SYSADMIN;

-- สร้าง User
create user if not exists identifier($airbyte_username)
password = $airbyte_password
default_role = $airbyte_role
default_warehouse = $airbyte_warehouse;

grant role identifier($airbyte_role) to user identifier($airbyte_username);

-- ส่วนสร้าง Warehouse และ Database (ใช้ sysadmin)
use role sysadmin;

-- สร้าง Warehouse
create warehouse if not exists identifier($airbyte_warehouse)
warehouse_size = xsmall
warehouse_type = standard
auto_suspend = 60
auto_resume = true
initially_suspended = true;

-- สร้าง Database
create database if not exists identifier($airbyte_database);

-- ส่วนกำหนดสิทธิ์ (Permissions)

-- ให้สิทธิ์ใช้ Warehouse
grant USAGE
on warehouse identifier($airbyte_warehouse)
to role identifier($airbyte_role);

-- ให้สิทธิ์เป็น OWNERSHIP
-- สิทธิ์นี้จะทำให้ Airbyte สร้าง Schema (airbyte_internal) เองได้
grant OWNERSHIP
on database identifier($airbyte_database)
to role identifier($airbyte_role);

commit;
