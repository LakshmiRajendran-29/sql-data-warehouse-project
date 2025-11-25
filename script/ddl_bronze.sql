
/*

=======================================
DDL SQL Script: Create Bronze Tables
=======================================

Script Purpose:

    This Script creates tables in the 'bronze' database, dropping existing tables 
      if they already exist.
    Run this script to re-define the DDL Structure of 'bronze' Tables
====================================================================================

*/

USE DataWarehouse;


-- 6 DDL Tables for Bronze Layer (3 CRM and 3 ERP)

-- Create First Bronze Layer Table : DDL (CRM customer Information)
DROP DATABASE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info 
( 
	cst_id INTEGER,
    cst_key VARCHAR(50), 
    cst_firstname VARCHAR(50), 
    cst_lastname VARCHAR(50), 
    cst_material_status VARCHAR(50), 
    cst_gndr VARCHAR(50), 
    cst_create_date DATE 
);

-- Create Second Bronze Layer Table : DDL (CRM Product Information)
DROP DATABASE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
		prd_id	INTEGER,
		prd_key	VARCHAR(50),
		prd_nm	VARCHAR(50),
		prd_cost INTEGER,
		prd_line VARCHAR(50),
		prd_start_dt DATE,	
		prd_end_dt DATE
);

-- Create Third Bronze Layer Table : DDL (CRM Sales Details)
DROP DATABASE IF EXISTS bronze.crm_sales_details;
CREATE Table bronze.crm_sales_details 
(
sls_ord_num	VARCHAR(50),
sls_prd_key	VARCHAR(50),
sls_cust_id	INTEGER,
sls_order_dt DATE,
sls_ship_dt	DATE,
sls_due_dt	DATE,
sls_sales	INTEGER,
sls_quantity INTEGER,
sls_price DECIMAL(10,2)
);

-- Create fourth Bronze Layer Table : DDL (ERP CUST_AZ12 )
DROP DATABASE IF EXISTS bronze.erp_cust_az12;
CREATE Table bronze.erp_cust_az12
(
CID	VARCHAR(50),
BDATE	DATE,
GEN VARCHAR(50)
);

-- Create Fifth Bronze Layer Table : DDL (ERP LOC_A101 )
DROP DATABASE IF EXISTS bronze.erp_loc_a101;
CREATE Table bronze.erp_loc_a101
(
CID	VARCHAR(50),
CNTRY VARCHAR(50)
);


-- Create Sixth Bronze Layer Table : DDL (ERP PX_CAT_G1V2 )
DROP DATABASE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE Table bronze.erp_px_cat_g1v2
(
ID	VARCHAR(50),	
CAT	VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50)
);

use bronze;
show tables;

select * from crm_cust_info LIMIT 10;
select Count(cst_firstname) from crm_cust_info; -- 18494
desc crm_cust_info;

select * from crm_prd_info LIMIT 10;
select Count(*) from crm_prd_info; -- 397
desc crm_prd_info;

select * from crm_sales_details LIMIT 10;
select Count(*) from crm_sales_details; -- 60398
desc crm_sales_details;

SELECT COUNT(*) FROM erp_cust_az12; -- 18484

SELECT COUNT(*) FROM erp_loc_a101; -- 18484

SELECT COUNT(*) FROM erp_px_cat_g1v2; -- 37
