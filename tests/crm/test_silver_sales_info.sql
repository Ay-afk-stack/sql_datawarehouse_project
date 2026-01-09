SET search_path TO silver;
-- Check for Invalid Date
SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0 
  OR LENGTH(sls_due_dt::TEXT) != 8 
  OR sls_due_dt > 20500101 
  OR sls_due_dt < 19000101;



-- Check for Invalid Data Orders
SELECT
  sls_order_dt, 
  sls_ship_dt,
  sls_due_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt > sls_due_dt 
  OR sls_order_dt > sls_due_dt 
  OR sls_order_dt > sls_ship_date;

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> sales: Quantity * Price
-- >> Values must not be NULL, Zero or Negative

SELECT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
  OR (sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL)
  OR (sls_sales <= 0 OR sls_quantity <=0 OR sls_price <=0);

-- Final Check

SELECT *
FROM silver.crm_sales_details;