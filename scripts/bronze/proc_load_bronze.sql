/*
==============================================================================================
Stored Procedure: Load Bronze Layer Tables (Source -> Bronze )
==============================================================================================
Script Purpose:
	This stored procedure load data into the 'bronze' schema from external csv files.
	It performs the following actions:
	- Truncates the bronze schema tables before loading data.
	- Uses the 'BULK INSERT' command to load data from csv files to bronze schema tables.
	- It prints the load duration for individual tables as well as for the entire batch load.

Parameters:
	None.
	This stored procedue does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze;

==============================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @startTime DATETIME, @endTime DATETIME, @batchStartTime DATETIME, @batchEndTime DATETIME;
	BEGIN TRY
		SET @batchStartTime = GETDATE();
		PRINT '=============================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================================================';


		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-----------------------------------------------------------------------------';


		SET @startTime = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'P:\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(VARCHAR,DATEDIFF(second, @startTime, @endTime)) + ' seconds';
		PRINT '>> ----------------------------------------';


		SET @startTime = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'P:\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(VARCHAR,DATEDIFF(second, @startTime, @endTime)) + ' seconds';
		PRINT '>> ----------------------------------------';



		SET @startTime = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'P:\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(VARCHAR,DATEDIFF(second, @startTime, @endTime)) + ' seconds';
		PRINT '>> ----------------------------------------';

		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '-----------------------------------------------------------------------------';



		SET @startTime = GETDATE();
		PRINT '>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'P:\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(VARCHAR,DATEDIFF(second, @startTime, @endTime)) + ' seconds';
		PRINT '>> ----------------------------------------';



		SET @startTime = GETDATE();
		PRINT '>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'P:\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(VARCHAR,DATEDIFF(second, @startTime, @endTime)) + ' seconds';
		PRINT '>> ----------------------------------------';



		SET @startTime = GETDATE();
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'P:\Data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(VARCHAR,DATEDIFF(second, @startTime, @endTime)) + ' seconds';
		PRINT '>> ----------------------------------------';

		SET @batchEndTime = GETDATE();
		PRINT '=============================================================================';
		PRINT ' Loading Bronze Layer is Completed';
		PRINT '- Total Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(second, @batchStartTime, @batchEndTime)) + ' seconds';
		PRINT '=============================================================================';

	END TRY
	BEGIN CATCH
		PRINT '=============================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CONVERT(NVARCHAR,ERROR_NUMBER());
		PRINT 'Error State: ' + CONVERT(NVARCHAR, ERROR_STATE());
		PRINT '=============================================================================';
	END CATCH
END
