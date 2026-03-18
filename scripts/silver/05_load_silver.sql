
/*
===============================================================================
PIPELINE: Bronze to Silver (ETL)
===============================================================================
Purpose: Transform messy 'Bronze' data into clean 'Silver' data.
Actions: Truncate -> Cleanse (TRIM, Cast, Deduplicate) -> Load
===============================================================================



===============================================================================
SILVER LAYER LOAD: crm_cust_info
===============================================================================
Transformations Applied:
1. String Cleaning: Removed leading/trailing whitespace from text columns.
2. Data Standardization/Normalization: Mapped inconsistent categorical codes 
   (e.g., 'M', 'F', 'S') to standardized, readable words ('Male', 'Female', 'Single').
3. Missing Value Handling: Replaced NULLs or blank categories with 'n/a'.
4. Data Integrity (Filtering): Dropped records with invalid or missing Primary Keys (0 or NULL).
5. Deduplication: Used Window Functions to keep only the most recent record per customer.
6. Metadata Enrichment: Added 'dwh_create_date' for pipeline auditing.
===============================================================================
*/


-- 1. Empty the table first (just in case you need to run this multiple times)
TRUNCATE TABLE silver.crm_cust_info;

-- 2. Insert the cleaned data
INSERT INTO silver.crm_cust_info (
    cst_id, 
    cst_key, 
    cst_firstname, 
    cst_lastname, 
    cst_marital_status, 
    cst_gndr,
    cst_create_date,
    dwh_create_date -- The audit column metadata!
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,  -- CLEANING: Removes hidden spaces
    TRIM(cst_lastname) AS cst_lastname,    -- CLEANING: Removes hidden spaces
    
    -- CLEANING: Standardize Marital Status
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status, 
    
    -- CLEANING: Standardize Gender
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr, 
    
    cst_create_date,
    CURRENT_TIMESTAMP AS dwh_create_date
FROM (
    -- CLEANING: Remove Duplicates by grabbing only the most recent record
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL AND cst_id != 0
) AS subquery
WHERE flag_last = 1;  -- Deduplication: Keep only the #1 most recent record




/*
===============================================================================
SILVER LAYER LOAD: crm_prd_info
===============================================================================
Transformations Applied:
1. String Manipulation: Split 'prd_key' into 'cat_id' and a clean 'prd_key'.
2. Missing Value Handling: Replaced NULL 'prd_cost' values with 0.
3. Data Standardization: Mapped categorical 'prd_line' codes to descriptive words.
4. Temporal Calculation: Calculated 'prd_end_dt' using Window Functions (LEAD) 
   to find the day before the next product version's start date.
5. Metadata Enrichment: Added 'dwh_create_date' for pipeline auditing.
===============================================================================
*/

-- 1. Empty the table first 
TRUNCATE TABLE silver.crm_prd_info;

-- 2. Insert the cleansed and transformed data
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    dwh_create_date
)
SELECT
    prd_id,
    
    -- String Manipulation: Extract Category ID and replace hyphen with underscore
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
    
    -- String Manipulation: Extract the actual Product Key 
    SUBSTRING(prd_key, 7) AS prd_key,        
    
    TRIM(prd_nm) AS prd_nm,
    
    -- Missing Value Handling: Replace NULL cost with 0
    IFNULL(prd_cost, 0) AS prd_cost,
    
    -- Data Standardization: Product Line
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line, 
    
    -- Date Casting
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    
    -- Temporal Calculation: The End Date is 1 day before the NEXT Start Date
    DATE_SUB(
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE), 
        INTERVAL 1 DAY
    ) AS prd_end_dt,

    CURRENT_TIMESTAMP AS dwh_create_date
FROM bronze.crm_prd_info;




/*
===============================================================================
SILVER LAYER LOAD: crm_sales_details
===============================================================================
Transformations Applied:
1. Data Type Conversion: Cast integer dates (YYYYMMDD) into standard MySQL DATE formats.
2. Missing/Invalid Value Handling: Converted date values of '0' or invalid lengths to NULL.
3. Data Quality (Math Validation): 
   - Recalculated 'sls_sales' if original value was missing, zero, or mathematically incorrect.
   - Derived 'sls_price' from sales and quantity if the original price was invalid.
===============================================================================
*/

-- 1. Empty the table first to allow for repeatable pipeline runs
TRUNCATE TABLE silver.crm_sales_details;

-- 2. Insert the cleansed and transformed data
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price,
    dwh_create_date
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    
    -- Data Type Conversion & Missing Value Handling: Order Date
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d')
    END AS sls_order_dt,
    
    -- Data Type Conversion & Missing Value Handling: Ship Date
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d')
    END AS sls_ship_dt,
    
    -- Data Type Conversion & Missing Value Handling: Due Date
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d')
    END AS sls_due_dt,
    
 -- Data Quality Validation: Sales Recalculation
   
    CASE 
        WHEN (sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)) 
             AND sls_price IS NOT NULL AND sls_price > 0 -- The Security Guard!
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales -- If price is broken, keep the original sales number
    END AS sls_sales, 
    
    sls_quantity,
    
    -- Data Quality Validation: Price Derivation
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price  
    END AS sls_price,

    CURRENT_TIMESTAMP AS dwh_create_date 

FROM bronze.crm_sales_details;





/*
===============================================================================
SILVER LAYER LOAD: erp_cust_az12
===============================================================================
Transformations Applied:
1. String Manipulation: Removed 'NAS' prefix from Customer IDs (cid) to align 
   with CRM ID formatting.
2. Data Quality (Outlier Handling): Identified and removed future birthdates 
   (bdate > CURRENT_DATE), replacing them with NULL.
3. Data Standardization: Mapped inconsistent gender codes ('F', 'FEMALE', 'M', 
   'MALE') to standardized values ('Male', 'Female', 'n/a').
4. Metadata Enrichment: Added 'dwh_create_date' for pipeline auditing.
===============================================================================
*/

-- 1. Empty the table first
TRUNCATE TABLE silver.erp_cust_az12;

-- 2. Insert the cleansed and transformed data
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen,
    dwh_create_date
)
SELECT
    -- String Manipulation: Remove 'NAS' prefix if it exists
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) 
        ELSE cid
    END AS cid, 
    
    -- Data Quality: Set future birthdates to NULL
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate, 
    
    -- Data Standardization: Clean Gender Categories
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen,
    
    CURRENT_TIMESTAMP AS dwh_create_date

FROM bronze.erp_cust_az12;




/*
===============================================================================
SILVER LAYER LOAD: erp_loc_a101
===============================================================================
Transformations Applied:
1. String Manipulation: Removed hyphens ('-') from Customer IDs (cid) to align 
   with CRM ID formatting.
2. Data Standardization: Mapped inconsistent country codes ('US', 'USA') to 
   a standardized value ('United States').
3. Missing Value Handling: Replaced empty or NULL countries with 'n/a'.
4. Metadata Enrichment: Added 'dwh_create_date' for pipeline auditing.
===============================================================================
*/

-- 1. Empty the table
TRUNCATE TABLE silver.erp_loc_a101;

-- 2. Insert the cleansed and transformed data
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry,
    dwh_create_date
)
SELECT
    -- String Manipulation: Remove all hyphens
    REPLACE(cid, '-', '') AS cid, 
    
    -- Data Standardization & Missing Value Handling
    CASE
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry, 
    
    CURRENT_TIMESTAMP AS dwh_create_date

FROM bronze.erp_loc_a101;



/*
===============================================================================
SILVER LAYER LOAD: erp_px_cat_g1v2
=============================================================================== 
*/

-- 1. Empty the table
TRUNCATE TABLE silver.erp_px_cat_g1v2;


-- 2. Insert the cleansed and transformed data
INSERT INTO silver.erp_px_cat_g1v2 (
    id, cat, subcat, maintenance
)
SELECT 
    id, 
    cat, 
    subcat, 
    maintenance
FROM bronze.erp_px_cat_g1v2;


SELECT * FROM silver.erp_px_cat_g1v2;




