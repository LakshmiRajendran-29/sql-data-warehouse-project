SELECT * FROM crm_cust_info;
SELECT * FROM crm_prd_info;
SELECT * FROM crm_sales_details;
SELECT * FROM erp_cust_az12;
SELECT * FROM erp_loc_a101;
SELECT * FROM erp_px_cat_g1v2;


-- prepare data for Gold Layer
-- To check the duplicates after joining the Tables 
SELECT cst_id, COUNT(*)
FROM
(SELECT 
	ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN erp_loc_a101 la
ON ci.cst_key = la.cid)t
GROUP BY cst_id
HAVING COUNT(*) >1;

-- 1.Joining the crm customer info, erp cust az12, erp loc g1v2
-- 2. Data Integration for Gender which is avaiable in two tables so we are going to integreate or merge 
-- 3.create object as a view for gold.dim_customers
-- 4. Check the Quality of the new Object


CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY ci.cst_id)  AS customer_key,
	ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
	la.cntry As country,
    ci.cst_marital_status AS marital_status,
	CASE 
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN erp_loc_a101 la
ON ci.cst_key = la.cid;

-- Data Integration for Gender which is avaiable in two tables so we are going to integreate or merge 

SELECT distinct
    ci.cst_gndr,
    ca.gen,
    CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,'n/a')
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN erp_loc_a101 la
ON ci.cst_key = la.cid
WHERE  ci.cst_gndr != ca.gen;


-- 4. Check the Quality of the new Object

SELECT distinct gender FROM gold.dim_customers;

-- -- 1.Joining the crm product info, erp loc g1v2 using prd_key and id
-- 2. Data Integration for Gender which is avaiable in two tables so we are going to integreate or merge 
-- 3.create object as a view for gold.dim_customers
-- 4. Check the Quality of the new Object

SELECT * FROM crm_prd_info;

SELECT * FROM erp_px_cat_g1v2;

-- Check duplicate after joining
SELECT prd_id,COUNT(*)
FROM (SELECT 
		pi.prd_id,
        pi.cat_id,
        pi.prd_key,
        pi.prd_nm,
        pi.prd_line,
        pi.prd_start_dt,
        pi.prd_end_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
FROM crm_prd_info pi
LEFT JOIN erp_px_cat_g1v2 pc
on pi.cat_id = pc.id)t
GROUP BY prd_id
HAVING COUNT(*) >2;

-- sort the column into logical groups to improve readbility
CREATE VIEW gold.dim_products AS 
SELECT 
		ROW_NUMBER() OVER(ORDER BY pf.prd_start_dt,pf.prd_key) as product_key,
		pf.prd_id AS product_id,
        pf.prd_key AS product_number,
        pf.prd_nm AS product_name,
        pf.cat_id AS category_id,
        pc.cat AS category,
        pc.subcat AS subcategory,
		pc.maintenance AS maintenance,
        pf.prd_cost AS product_cost,
        pf.prd_line AS product_line,
        pf.prd_start_dt AS start_date
FROM crm_prd_info pf
LEFT JOIN erp_px_cat_g1v2 pc
on pf.cat_id = pc.id
WHERE pf.prd_end_dt IS NULL; -- filter out all historical data


-- BUilding fact Table for the gold Layer

SELECT * FROM crm_prd_info;
SELECT * FROM crm_sales_details;


SELECT 
		sd.sls_ord_num,
        sd.sls_prd_key,
        sd.sls_cust_id,
        sd.sls_order_dt,
        sd.sls_ship_dt,
        sd.sls_due_dt,
        sd.sls_sales,
        sd.sls_quantity,
        sd.sls_price,
        pf.prd_key
FROM crm_sales_details sd
LEFT JOIN crm_prd_info pf
ON sd.sls_prd_key = pf.prd_key;

-- create the fact table in gold layer
CREATE VIEW gold.fact_sales AS
SELECT 
		sd.sls_ord_num AS order_number,
        gp.product_key ,
        gc.customer_key,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt AS shipping_date,
        sd.sls_due_dt AS due_date,
        sd.sls_sales AS sales_amount,
        sd.sls_quantity AS quantity,
        sd.sls_price AS price
FROM crm_sales_details sd
LEFT JOIN gold.dim_products gp
ON sd.sls_prd_key = gp.product_number
LEFT JOIN gold.dim_customers gc
ON sd.sls_cust_id = gc.customer_id;	

-- Check the Quality of the Fact Table in Gold Layer

-- Check for the Foreign Key Integrity
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL;
