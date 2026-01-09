TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT 
  CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ElSE cid
  END AS cid,
  CASE WHEN bdate > CURRENT_DATE THEN NULL
       ELSE bdate -- Set Future birthdates as NULL
  END AS bdate,
  CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
       WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
       ELSE 'n/a'
  END AS gen -- Normalize Gender Values and handle unknown cases
FROM bronze.erp_cust_az12;
