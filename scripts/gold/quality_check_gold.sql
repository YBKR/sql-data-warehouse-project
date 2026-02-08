/* 
==================================================================================================================================
Quality Checks
==================================================================================================================================
  Script Purpose:
  
      This script performs quality checks to validate the integrity, consistency, and accuracy of the Gold Layer. These checks ensure:
      
      Uniqueness of surrogate keys in dimension tables.
      
      Referential integrity between fact and dimension tables.
      
      Validation of relationships in the data model for analytical purposes.
  
  Usage Notes:

      Run these checks after data loading Silver Layer.
  
  Investigate and resolve any discrepancies found during the checks.
==================================================================================================================================
*/

SELECT cst_id, COUNT(*) FROM (
	SELECT

		ROW_NUMBER () OVER ( ORDER BY cst_id) AS customer_key,
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid

)t
GROUP BY cst_id
HAVING COUNT(*) > 1


SELECT * from silver.crm_cust_info;

SELECT * from gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key,
	pn.prd_nm,
	pc.cat,
	pc.subcat,
	pc.maintenance,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt is NULL; ---- Filtering out the historical data


SELECT
	sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sls_order_dt AS order_date,
	sls_ship_dt AS shipping_date,
	sls_due_dt AS due_date,
	sls_sales AS sales_amount,
	sls_quantity AS quantity,
	sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number --- AS product_key
LEFT JOIN gold.dim_customers Cu
ON sd.sls_cust_id = cu.customer_id --- AS customer_key
