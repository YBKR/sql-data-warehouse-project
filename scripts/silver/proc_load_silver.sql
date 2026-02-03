/*

**********************************************************************************
Stored Procedure: Load Silver Layer (Bronze -> Silver)
**********************************************************************************

Script Purpose:

        This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the 'bronze schema.
      
    Actions Performed:
      
        - Truncates Silver tables.
      
        -  Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:

            None.

            This stored procedure does not accept any parameters or return any values.

Usage Example:

                EXEC Silver, load_silver;
*****************************************************************************************
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT' ============================================= ';
		PRINT' Loading SILVER Layer';
		PRINT ' ============================================= ';

		PRINT ' --------------------------------------------- '
		PRINT' Loading CRM tables';
		PRINT ' --------------------------------------------- '

		--- 1. Loading the cleaned data into silver.crm_cust_info

		SET @start_time = GETDATE();
		PRINT '>>> Truncating silver.erp_loc_a101';
		TRUNCATE TABLE silver.crm_cust_info;

		--- SELECT * FROM silver.crm_cust_info;

		PRINT '>>> Inserting data into silver.erp_loc_a101';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date )

		SELECT	
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,

			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'n/a'
			END cst_marital_status,

			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr)) = 'S' THEN 'Female'
				 ELSE 'n/a'
			END cst_gndr,
			cst_create_date

			FROM (
				SELECT 
				*, 
				ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
			)t WHERE flag_last = 1;

			SET @end_time = GETDATE();
			PRINT '>>> Loading duration of silver.crm_cust_info table : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
			PRINT '------------------------------';
			PRINT '';


		--- 2. Loading the cleaned data into silver.crm_prd_info

		SET @start_time = GETDATE();
		PRINT '>>> Truncating silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		--- Select * from silver.crm_prd_info;

		PRINT '>>> Inserting data into silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL (prd_cost, 0) AS prd_cost,
			CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Montain'
				 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				 ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_ster_dt,
			DATEADD(
				DAY,
				-1,
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of silver.crm_cust_info table : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';


		--- 3. Loading the cleaned data into silver.crm_prd_info

		SET @start_time = GETDATE();
		PRINT '>>> Truncating silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		--- Select * from silver.crm_sales_details;

		PRINT '>>> Inserting data into silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details ( 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)

		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,

			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,

			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,

			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
					  THEN sls_quantity *ABS(sls_price)
				 ELSE sls_sales
			END new_sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
					  THEN sls_sales / NULLIF(sls_quantity, 0)
				 ELSE sls_price
			END new_sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of silver.crm_cust_info table : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';


		PRINT ' --------------------------------------------- '
		PRINT' Loading ERP tables';
		PRINT ' --------------------------------------------- '


		--- 4. Loading the cleaned data into silver.crm_prd_info

		SET @start_time = GETDATE();
		PRINT '>>> Truncating silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_cust_az12;

		--- Select * from silver.erp_cust_az12;
		PRINT '>>> Inserting data into silver.erp_loc_a101';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)

		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				 ELSE cid
			END AS cid,

			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END AS badte,

			CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				 ELSE 'n/a'
			END AS gen

		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of silver.crm_cust_info table : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';



		--- 5. Loading the cleaned data into silver.crm_prd_info

		SET @start_time = GETDATE();
		PRINT '>>> Truncating silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		--- Select * from silver.erp_loc_a101;
		PRINT '>>> Inserting data into silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid, 
			cntry
		)

		SELECT
			REPLACE(cid, '-', '') AS cid,

			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of silver.crm_cust_info table : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';



		--- 6. Loading the cleaned data into silver.erp_px_cat_g1v2

		SET @start_time = GETDATE();
		PRINT '>>> Truncating silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		--- Select * from silver.erp_px_cat_g1v2;
		PRINT '>>> Inserting data into silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)

		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of silver.crm_cust_info table : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';
	END TRY

	BEGIN CATCH
		PRINT '===========================================';
		PRINT 'Error occured during loading Silver layer.....!!!! : ';
		PRINT 'Error message ' + ERROR_MESSAGE();
		PRINT 'Error number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error state ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================';
	END CATCH

	SET @batch_end_time = GETDATE();
	PRINT ' ******************************* ';
	PRINT ' Silver layer loading is completed........';
	PRINT '==> Duration of Silver layer loading : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
	PRINT ' ******************************* ';

END;

EXEC silver.load_silver;
