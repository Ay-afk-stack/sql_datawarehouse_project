-- Check for Nulls or Duplicates in Primary Key
-- Expectation : No Result
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--Check for unwanted Spacing
-- Expectation: No Result
SELECT *
FROM silver.crm_prd_info cpi 
WHERE TRIM(cpi.prd_nm) <> cpi.prd_nm;

-- Check Nulls or Negative Numbers
-- Expectation: NO results
SELECT prd_cost
FROM silver.crm_prd_info cpi 
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info cpi ;

-- Checking Invalid Date
SELECT *
FROM silver.crm_prd_info cpi
WHERE prd_end_dt < prd_start_dt;

-- Fixing Invalid Date Orders
SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
(LEAD(cpi.prd_start_dt) OVER(PARTITION BY cpi.prd_key ORDER BY cpi.prd_start_dt)) - 1
AS prd_end_dt_test
FROM bronze.crm_prd_info cpi 
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- Final Query
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
COALESCE(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) - 1
AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT *
FROM bronze.crm_prd_info cpi ;


DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT NOW()
);


-- Inserting into the silver schema database
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
prd_nm,
COALESCE(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) - 1
AS prd_end_dt
FROM bronze.crm_prd_info;

SELECT *
FROM silver.crm_prd_info cci ;






