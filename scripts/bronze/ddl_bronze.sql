
/*
===============================================================================
 Script Name : Create_Bronze_Tables.sql
 Description :
    This script creates raw (Bronze layer) tables for CRM and ERP source systems.
    It safely drops existing tables (if any) before recreating them to ensure
    a clean and consistent schema.

 Scope :
    - CRM customer, product, and sales detail tables
    - ERP location, customer, and product category tables

 Purpose :
    The Bronze layer stores raw, ingested data with minimal transformation.
    These tables act as the foundation for downstream Silver and Gold layers
    where data will be cleaned, enriched, and aggregated.

 Notes :
    - No primary keys or constraints are applied at this stage
    - Data types closely match source system formats
    - Designed for ELT / Data Warehouse pipelines

 Warning :
    ⚠️ Existing tables in the bronze schema will be dropped and recreated.

 Created   : 2026-01-28
 Schema    : bronze
 Database  : DataWarehouse
===============================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info
(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE,
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info
(
	prd_id				INT,
	prd_key				NVARCHAR(50),
	prd_nm				NVARCHAR(50),
	prd_cost			INT,
	prd_line			NVARCHAR(50),
	prd_start_dt		DATETIME,
	prd_end_dt			DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details 
(
	sls_ord_num			NVARCHAR(50),
	sls_prd_key			NVARCHAR(50),
	sls_cust_id			INT,
	sls_order_dt		INT,
	sls_ship_dt			INT,
	sls_due_dt			INT,
	sls_sales			INT,
	sls_quantity		INT,
	sls_price			INT,

);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101
(
	cid_id				NVARCHAR(50),
	cntry_nm			NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12
(
	cid_id				NVARCHAR(50),
	bdate				DATE,
	gen 				NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2
(
	id 				NVARCHAR(50),
	cat 			NVARCHAR(50),
	subcat 			NVARCHAR(50),
	maintenance 	NVARCHAR(50)
);
