/* 

*******************************************************************************************************************
  Quality Checks
*******************************************************************************************************************
    Script Purpose:
    
            - This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schema. It includes checks for:
            
            - Null or duplicate primary keys.
            
            - Unwanted spaces in string fields.
            
            - Data standardization and consistency.
            
            - Invalid date ranges and orders.
            
            - Data consistency between related fields.
    
    Usage Notes:
    
            - Run these checks after data loading Silver Layer.
            - Investigate and resolve any discrepancies found during the checks.

********************************************************************************************************************************

*/



/*
	Quality check
*/


/*
	Q1. Quality check for the crm_cust_info
*/

---- Q1A.Check for the duplicates in Primary key
---- Expectratin : No result
SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

---- Q1B.Check for the Un-Wanted spaces in Names and Marital Status and Gender
---- Expectratin : No result

SELECT
	cst_firstname
FROM silver.crm_cust_info
WHERE 
	cst_firstname != TRIM(cst_firstname)

SELECT
	cst_lastname
FROM silver.crm_cust_info
WHERE 
	cst_lastname != TRIM(cst_lastname)

SELECT
	cst_gndr
FROM silver.crm_cust_info
WHERE 
	cst_gndr != TRIM(cst_gndr)

SELECT
	cst_marital_status
FROM silver.crm_cust_info
WHERE 
	cst_marital_status != TRIM(cst_marital_status)



SELECT 
	*, 
	ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL)t 
	WHERE flag_last = 1


/*
	Q2. Quality check for the crm_prd_info
*/

--- Q2A.Adjusting the full name sfor thr short cuts or Observations

SELECT 
	prd_id,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Montain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
FROM bronze.crm_prd_info;

--- Q2B.Spliting and adding the Categery_id and product_key separatle from the Product_key column

SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	ISNULL (prd_cost, 0) AS prd_cost,
FROM bronze.crm_prd_info;


/* 
   Q2C1.Adjusting and cleanning the date's, 
   Q2C2.adding the end dates based on the next start date
   Q2C3.if product has to are more orders or start's dates by removing the 1 day from the next start day
*/

SELECT 
	prd_id,
	CAST(prd_start_dt AS DATE) AS prd_ster_dt,
	DATEADD(
		DAY,
		-1,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
	--- CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;


/*
	Q3. Quality check for the crm_sales_details
*/

---- Q3A.Check for the any Un-Wanted spacess exist in the table data

SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)


---- Q3B.Check for the quality issues in the sls_order_dt

SELECT 
	sls_order_dt
FROM bronze.crm_sales_details
--- WHERE sls_ord_num != TRIM(sls_ord_num)

---- Q3C.Check for the quality issues in the sls_ship_dt

SELECT 
	sls_ship_dt
FROM bronze.crm_sales_details
--- WHERE sls_ord_num != TRIM(sls_ord_num)

---- Q3D.Check for the quality issues in the sls_due_dt

SELECT 
	sls_due_dt
FROM bronze.crm_sales_details
--- WHERE sls_ord_num != TRIM(sls_ord_num)


/* 
	For the following cases bad  quality of sales date exist's, Based on the Business rules we can fix it with business experts suggestions

	A. If sales having nulls or negetive or zero's derive it using the quanity and price
	B. If price is zero or Null or negetive, calculate it with using quantity and sales
	C. If price is negetive , convert it with positive
*/


SELECT 
	sls_sales,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
			  THEN sls_quantity *ABS(sls_price)
		 ELSE sls_sales
	END new_sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales


SELECT 
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
			  THEN sls_quantity *ABS(sls_price)
		 ELSE sls_sales
	END new_sls_sales,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
			  THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END new_sls_price,
	sls_quantity
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales



/*
	Q4. Quality check for the erp_cust_az12
*/


SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid
FROM bronze.erp_cust_az12;

SELECT
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END AS badte
FROM bronze.erp_cust_az12;

SELECT DISTINCT
	CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12




/*
	Q5. Quality check for the erp_loc_a101
*/

SELECT
	REPLACE(cid, '-', '') AS cid,
FROM bronze.erp_loc_a101;

SELECT
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;



/*
	Q5. Quality check for the silver.erp_px_cat_g1v2
*/

SELECT *
FROM bronze.erp_px_cat_g1v2;


