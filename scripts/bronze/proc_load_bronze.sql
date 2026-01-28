/*
===============================================================================
 Procedure Name : bronze.load_bronze
 Description :
    Loads raw CRM and ERP source data into the Bronze layer of the
    DataWarehouse using BULK INSERT operations.

    For each table, the procedure:
      - Truncates existing data
      - Loads fresh data from CSV files
      - Logs load duration for monitoring and debugging purposes

 Source Systems :
    - CRM : Customer, Product, Sales Details
    - ERP : Location, Customer, Product Category

 Features :
    - Batch-level and table-level execution time tracking
    - TRY...CATCH error handling
    - Console logging using PRINT statements
    - Optimized bulk load using TABLOCK

 Purpose :
    The Bronze layer stores raw, untransformed data exactly as received
    from source systems. This procedure is intended to be the first step
    in an ELT pipeline before Silver and Gold transformations.

 Warning :
    ⚠️ This procedure TRUNCATES all Bronze tables before loading data.
    Ensure source files are validated and available before execution.

 Execution :
    EXEC bronze.load_bronze;

 Created   : 2026-01-28
 Schema    : bronze
 Database  : DataWarehouse
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @Start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------';

		SET @Start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>Inserting Data: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\CRMERP\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------------';

		SET @Start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>Inserting Data: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\CRMERP\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------------'


		SET @Start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>Inserting Data: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\CRMERP\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------------'

		PRINT '---------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------';

		SET @Start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>Inserting Data: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\CRMERP\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------------'

		SET @Start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>Inserting Data: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\CRMERP\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------------'

		SET @Start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>Inserting Data: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\CRMERP\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '---------------------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Bronze Layer Load Completed Successfully';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '=============================================';

	END TRY
	BEGIN CATCH
		PRINT '=============================================';
		PRINT 'Error occurred while loading Bronze Layer: '
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR(50));
		PRINT '=============================================';
	END CATCH;
END



