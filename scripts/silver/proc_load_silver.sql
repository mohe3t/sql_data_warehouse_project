--EXEC silver.load_silver ; 
/*
==============================================================================================
Stored Procedure: Load Silver Layer Tables (Bronze -> Silver )
==============================================================================================
Script Purpose:
	This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 
  'silver' schema tables from the 'bronze' schema.
	Actions Performed:
	- Truncates the silver schema tables before loading data.
	- Inserts transformed and cleaned data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedue does not accept any parameters or return any values.

Usage Example:
	EXEC silver.load_silver;

==============================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @batchStartTime DATETIME, @batchEndTime DATETIME, @loadStartTime DATETIME, @loadEndTime DATETIME
	BEGIN TRY
		SET @batchStartTime = GETDATE();
		PRINT '=============================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================================================================';

		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-----------------------------------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @loadStartTime = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status, 
			cst_gndr, 
			cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,  -- Removing Unwanted Spaces
			TRIM(cst_lastname) AS cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))   -- Data Normalization & Standardization
				WHEN 'M' THEN 'Married'
				WHEN 'S' THEN 'Single'
				ELSE 'n/a'					 	   -- Handled Missing Values																				
			END AS cst_marital_status,
			CASE UPPER(TRIM(cst_gndr))
				WHEN 'M' THEN 'Male'
				WHEN 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
			  FROM bronze.crm_cust_info 
			  WHERE cst_id IS NOT NULL) t 
		WHERE flag_last =1						     -- Removed Duplicates using Data Filtering

		SET @loadEndTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(SECOND, @loadStartTime, @loadEndTime)) + ' seconds.'
		PRINT '>> ---------------------------------';

		-- Loading silver.crm_prd_info
		SET @loadStartTime = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
		SELECT prd_id
			  ,REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id				-- Derived Column
			  ,SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key				        -- Derived Column
			  ,prd_nm
			  ,ISNULL(prd_cost,0) AS prd_cost				-- Handling Missing info
			  ,CASE UPPER(TRIM(prd_line))				    -- Data Normalization
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			  END AS prd_line
			  ,prd_start_dt
			  ,DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt 				-- Data Enrichment
		 FROM DataWarehouse.bronze.crm_prd_info

		SET @loadEndTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(SECOND, @loadStartTime, @loadEndTime)) + ' seconds.'
		PRINT '>> ---------------------------------';

		-- Loading silver.crm_sales_details
		SET @loadStartTime = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		SELECT sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id
			  ,CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL					-- Data Transformation
				ELSE CONVERT(DATE,CONVERT(NVARCHAR,sls_order_dt))					        -- Data Tyep Casting
				END AS sls_order_dt
			  ,CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CONVERT(DATE,CONVERT(NVARCHAR,sls_ship_dt))
				END AS sls_ship_dt
			  ,CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CONVERT(DATE,CONVERT(NVARCHAR,sls_due_dt))
				END AS sls_due_dt
			  ,CASE 	
					WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)	-- Handling Invalid/Missing Data, Deriving Value
					ELSE sls_sales
				END AS sls_sales
			  ,sls_quantity
			  ,CASE 
					WHEN sls_price <=0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
				END AS sls_price 
		  FROM DataWarehouse.bronze.crm_sales_details
		
		SET @loadEndTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(SECOND, @loadStartTime, @loadEndTime)) + ' seconds.'

		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-----------------------------------------------------------------------------';

		-- Loading silver.erp_cust_az12
		SET @loadStartTime = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
		SELECT
				CASE 
					-- Remove 'NAS' prefix if present
					WHEN cid LIKE 'NAS%' THEN  SUBSTRING(cid,4,LEN(cid)) 
					ELSE cid
				END AS cid
			  ,CASE
					-- Setting future birthdates to Null
					WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END AS bdate
			  ,CASE
					-- Normalize gender values and handle unknown values
					WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN 'Male'
					ELSE 'n/a'
				END AS gen
		  FROM [DataWarehouse].[bronze].[erp_cust_az12]
		
		SET @loadEndTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(SECOND, @loadStartTime, @loadEndTime)) + ' seconds.'
		PRINT '>> ---------------------------------';

		-- Loading silver.erp_loc_a101
		SET @loadStartTime = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT 
			   -- Removed unwanted character from cid
			   REPLACE(cid, '-','') AS cid
			  ,CASE 
				-- Normalized and Handled missing or invalid country codes
				WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE cntry
			END AS cntry
		  FROM [DataWarehouse].[bronze].[erp_loc_a101]
		
		SET @loadEndTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(SECOND, @loadStartTime, @loadEndTime)) + ' seconds.'
		PRINT '>> ---------------------------------';

		-- Loading silver.erp_px_cat_g1v2
		SET @loadStartTime = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT id
			  ,TRIM(cat) AS cat
			  ,TRIM(subcat) AS subcat
			  ,TRIM(maintenance) AS maintenance
		  FROM DataWarehouse.bronze.erp_px_cat_g1v2

		SET @loadEndTime = GETDATE();
		PRINT '>> Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(SECOND, @loadStartTime, @loadEndTime)) + ' seconds.'
		PRINT '>> ---------------------------------';

		SET @batchEndTime = GETDATE();
		PRINT '=============================================================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '>> Total Load Duration: ' + CONVERT(NVARCHAR,DATEDIFF(SECOND, @batchStartTime, @batchEndTime)) + ' seconds.'
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
