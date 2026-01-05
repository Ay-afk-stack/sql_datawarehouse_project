/* 
===========================\
    Creating Bulk Insert
===========================
Script Purpose:
    This script bulk inserts the value from csv files.
    This script also do full load in the table to update new datas in it.
    This script also have a Stored Procedure for quick execution.
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
        RAISE NOTICE 'Loading Bronze Layer...';
    BEGIN
        RAISE NOTICE 'Loading CRM Tables';    
        RAISE NOTICE '>>> Truncating Table: crm_cust_info';    
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE '>>> Inserting into Table: crm_cust_info';    
        COPY bronze.crm_cust_info
        FROM '/datasets/source_crm/cust_info.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );

        -- crm_prd_info
        RAISE NOTICE '>>> Truncating Table: crm_prd_info';    
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE '>>> Inserting into Table: crm_prd_info';    
        COPY bronze.crm_prd_info
        FROM '/datasets/source_crm/prd_info.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );

        -- crm_sales_details
        RAISE NOTICE '>>> Truncating Table: crm_sales_details';    
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE '>>> Inserting into Table: crm_sales_details';    
        COPY bronze.crm_sales_details
        FROM '/datasets/source_crm/sales_details.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Failed Loading ERP Table...';
    
    END;

        RAISE NOTICE 'Loading ERP Tables';

    BEGIN
        -- erp_cust_az12
        RAISE NOTICE '>>> Truncating Table: erp_cust_az12';    
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>>> Inserting into Table: erp_cust_az12';    
        COPY bronze.erp_cust_az12
        FROM '/datasets/source_erp/CUST_AZ12.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );

        -- erp_loc_a101
        RAISE NOTICE '>>> Truncating Table: erp_loc_a101'; 

        TRUNCATE TABLE bronze.erp_loc_a101;
        RAISE NOTICE '>>> Inserting into Table: erp_loc_a101';    

        COPY bronze.erp_loc_a101
        FROM '/datasets/source_erp/LOC_A101.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );

        -- erp_px_cat_g1v2
        RAISE NOTICE '>>>Truncating Table: erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>>>Inserting into Table: erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2
        FROM '/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FORMAT CSV,
            HEADER true,
            DELIMITER ','
        );  
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Failed Loading ERP table...';
    END;
END;
$$;

CALL bronze.load_bronze();