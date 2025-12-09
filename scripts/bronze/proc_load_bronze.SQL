/*

  =====================================================
  Stored Procedure: Load Bronze Layer (Source -> Bronze)
  ======================================================
  Script Purpose:
              -- This stored procedure loads data into the 'bronze' schema from external CSV files.
              -- It performs the following actions:
                --> Truncates the bronze tables before loading data.
                --> Uses the BULK INSERT command to load data from csv Files to bronze tables.
  
  Parameters:
            None.
    This stored procedure does not accept any parameters or return any values.
  
  Usage Example:
    EXEC bronze.load_bronze;

  =========================================================  
*/


CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT' ============================================= ';
		PRINT' Loading BRONZE Layer';
		PRINT ' ============================================= ';

		PRINT ' --------------------------------------------- '
		PRINT' Loading CRM tables';
		PRINT ' --------------------------------------------- '

		SET @start_time = GETDATE();
		PRINT '>>> Truncating table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>> Inserting data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\1ques\Downloads\Data Warehouse Data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of bronze.crm_cust_info table : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';


		SET @start_time = GETDATE();
		PRINT '>>> Truncating table : bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT ' >>> Inserting Data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\1ques\Downloads\Data Warehouse Data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of bronze.crm_prd_info : ' + CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>> Inserting data into : bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\1ques\Downloads\Data Warehouse Data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of bronze.crm_sales_details : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';


		PRINT ' --------------------------------------------- '
		PRINT' Loading ERP tables';
		PRINT ' --------------------------------------------- '

		SET @start_time = GETDATE();
		PRINT '>>> Truncating table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>> Inserting data into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\1ques\Downloads\Data Warehouse Data\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>> Loading duration of bronze.erp_cust_az12 : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>> Inserting data into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\1ques\Downloads\Data Warehouse Data\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' >>> Loading duration of bronze.erp_loc_a101 : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>> Inserting data into : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\1ques\Downloads\Data Warehouse Data\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ' >>> Loading duration of bronze.erp_px_cat_g1v2 : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '------------------------------';
		PRINT '';
	END TRY

	BEGIN CATCH
		PRINT '===========================================';
		PRINT 'Error occured during loading bronze layer.....!!!! : ';
		PRINT 'Error message ' + ERROR_MESSAGE();
		PRINT 'Error number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error state ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================';
	END CATCH
	SET @batch_end_time = GETDATE();
	PRINT ' ******************************* ';
	PRINT ' Bronze layer loading is completed........';
	PRINT '==> Duration of Bronze layer loading : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
	PRINT ' ******************************* ';
END;
