-- Check for Unwanted Spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
  OR subcat != trim(subcat) 
  OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;


SELECT *
FROM silver.erp_px_cat_g1v2;