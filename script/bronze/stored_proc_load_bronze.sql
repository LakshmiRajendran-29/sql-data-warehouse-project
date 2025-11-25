/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - LOAD DATA LOCAL INFILE
    - Error handling rewritten using a CONDITION HANDLER

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL load_bronze();
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start DATETIME;
    DECLARE batch_end DATETIME;

    DECLARE exit handler for SQLEXCEPTION
    BEGIN
        SELECT 'ERROR WHILE LOADING BRONZE LAYER' AS message;
        RESIGNAL;
    END;

    SET batch_start = NOW();
    SELECT '================================================' AS msg;
    SELECT 'Loading Bronze Layer Started' AS msg;

    -- CRM Tables
    SELECT 'Loading CRM Tables...' AS msg;

    -- crm_cust_info
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_cust_info;
    LOAD DATA LOCAL INFILE '/path/DWH Project/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze.crm_cust_info
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    SET end_time = NOW();
    SELECT CONCAT('crm_cust_info Load Seconds: ', TIMESTAMPDIFF(SECOND, start_time, end_time)) AS msg;

    -- crm_prd_info
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_prd_info;
    LOAD DATA LOCAL INFILE '/path/DWH Project/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze.crm_prd_info
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    SET end_time = NOW();
    SELECT CONCAT('crm_prd_info Load Seconds: ', TIMESTAMPDIFF(SECOND, start_time, end_time)) AS msg;

    -- crm_sales_details
    SET start_time = NOW();
    TRUNCATE TABLE bronze.crm_sales_details;
    LOAD DATA LOCAL INFILE '/path/DWH Project/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze.crm_sales_details
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    SET end_time = NOW();
    SELECT CONCAT('crm_sales_details Load Seconds: ', TIMESTAMPDIFF(SECOND, start_time, end_time)) AS msg;

    -- ERP Tables
    SELECT 'Loading ERP Tables...' AS msg;

    -- erp_loc_a101
    SET start_time = NOW();
    TRUNCATE TABLE bronze.erp_loc_a101;
    LOAD DATA LOCAL INFILE '/path/DWH Project/datasets/source_erp/loc_a101.csv'
    INTO TABLE bronze.erp_loc_a101
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    SET end_time = NOW();
    SELECT CONCAT('erp_loc_a101 Load Seconds: ', TIMESTAMPDIFF(SECOND, start_time, end_time)) AS msg;

    -- erp_cust_az12
    SET start_time = NOW();
    TRUNCATE TABLE bronze.erp_cust_az12;
    LOAD DATA LOCAL INFILE '/path/DWH Project/datasets/source_erp/cust_az12.csv'
    INTO TABLE bronze.erp_cust_az12
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    SET end_time = NOW();
    SELECT CONCAT('erp_cust_az12 Load Seconds: ', TIMESTAMPDIFF(SECOND, start_time, end_time)) AS msg;

    -- erp_px_cat_g1v2
    SET start_time = NOW();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    LOAD DATA LOCAL INFILE '/path/DWH Project/datasets/source_erp/px_cat_g1v2.csv'
    INTO TABLE bronze.erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 LINES;
    SET end_time = NOW();
    SELECT CONCAT('erp_px_cat_g1v2 Load Seconds: ', TIMESTAMPDIFF(SECOND, start_time, end_time)) AS msg;

    SET batch_end = NOW();
    SELECT '===========================================' AS msg;
    SELECT CONCAT('Bronze Load Completed in ', TIMESTAMPDIFF(SECOND, batch_start, batch_end), ' seconds') AS msg;
    SELECT '===========================================' AS msg;

END$$

DELIMITER ;
