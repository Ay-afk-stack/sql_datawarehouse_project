SET search_path TO silver;

CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender info
    ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birth_date,
    ci.cst_create_date AS create_date
FROM crm_cust_info ci
LEFT JOIN erp_cust_az12 ca
         ON ci.cst_key = ca.cid
LEFT JOIN erp_loc_a101 la
         ON ci.cst_key = la.cid;

-- Calling View
SET search_path TO gold;
SELECT *
FROM gold.dim_customers;