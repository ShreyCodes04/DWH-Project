/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
BEGIN TRY
    PRINT '=======================================================';
    PRINT 'LOADING BRONZE LAYER';
    PRINT '=======================================================';
    -- CRM Customer Info

    PRINT '-------------------------------------------------------';
    PRINT 'LOADING CRM FILES';
    PRINT '-------------------------------------------------------';
    
    SET @start_time = GETDATE();
    PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
    BULK INSERT bronze.crm_cust_info
    FROM '/var/opt/mssql/data/cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';


    -- CRM Product Info
    SET @start_time = GETDATE();
    PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/data/prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

    -- CRM Sales Details
    SET @start_time = GETDATE();
    PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details';
    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/data/sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

    -- ERP Customer AZ12

    PRINT '-------------------------------------------------------';
    PRINT 'LOADING CRM FILES';
    PRINT '-------------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12s';
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
    BULK INSERT bronze.erp_cust_az12
    FROM '/var/opt/mssql/data/CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

    -- ERP LOC A101
    SET @start_time = GETDATE();
    PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/data/LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

    -- ERP PX CAT G1V2
    SET @start_time = GETDATE();
    PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

    SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
END TRY
BEGIN CATCH
    PRINT '======================================'
    PRINT 'Error ocuured while loading bronze layer';
    PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
    PRINT '======================================'
END CATCH
END
GO
