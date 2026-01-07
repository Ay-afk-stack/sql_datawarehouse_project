-- Quality Checks

-- Check for Nulls or Duplicates in Primary Key
-- Expectation : No Result
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check Nulls or Negative Numbers
-- Expectation: No results
SELECT prd_cost
FROM silver.crm_prd_info cpi 
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info cpi ;


-- Checking for duplication
-- Expectation: No results
SELECT *
FROM ( SELECT 
		*,	
		row_number() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM silver.crm_cust_info cci ) t
WHERE flag_last <> 1;

-- Checking Invalid Date
SELECT *
FROM silver.crm_prd_info cpi
WHERE prd_end_dt < prd_start_dt;