-- Checking Data
SELECT 
cid, 
cntry
FROM silver.erp_loc_a101;

-- Data Standardization & Consistency
select DISTINCT 
cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
   WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
   WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
   ELSE cntry
END AS cntry
FROM silver.erp_loc_a101;
