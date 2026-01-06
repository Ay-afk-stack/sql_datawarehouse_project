-- Expectation: No Results -> All good
SELECT cst_id , COUNT(*)
FROM silver.crm_cust_info cci 
GROUP BY cst_id
HAVING COUNT(*) > 1; 

-- Checking for nulls
SELECT *
FROM silver.crm_cust_info cci 
WHERE cst_id IS NULL;


-- Checking for duplication
SELECT *
FROM ( SELECT 
		*,	
		row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM silver.crm_cust_info cci ) t
WHERE flag_last <> 1;

-- Check for unwanted Spaces
SELECT 
cst_id, cst_firstname
FROM silver.crm_cust_info cci 
WHERE cci.cst_firstname != TRIM(cci.cst_firstname);