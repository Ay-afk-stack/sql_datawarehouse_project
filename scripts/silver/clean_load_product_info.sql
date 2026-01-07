-- Full Load(Truncate & Insert) into the silver schema database
TRUNCATE TABLE IF EXISTS silver.crm_prd_info;

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
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, -- Extract Category Id
SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key, -- Extract Product Key
prd_nm,
COALESCE(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	 WHEN 'M' THEN 'Mountain'
	 WHEN 'R' THEN 'Road'
	 WHEN 'S' THEN 'Other Sales'
	 WHEN 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line, -- Map Product line codes to descriptive values
CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Data Transformation using CAST() method
(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) - 1
AS prd_end_dt -- Calculate end date as one day before the next start date
FROM bronze.crm_prd_info;