# sql-data-warehouse-project
# 📊 SQL Data Warehouse Project (Bronze–Silver–Gold Architecture)

## Overview

This project implements an **end-to-end SQL Data Warehouse** using the **Medallion Architecture (Bronze → Silver → Gold)**. It demonstrates how raw data from multiple source systems (CRM & ERP) can be ingested, cleansed, transformed, and modeled into analytics-ready structures.

The project is designed to reflect **real-world data engineering practices**, including ELT pipelines, data quality handling, dimensional modeling, and performance-aware SQL design.

---

## Architecture

```
Source CSV Files
      ↓
  Bronze Layer (Raw Data)
      ↓
  Silver Layer (Cleansed & Conformed Data)
      ↓
  Gold Layer (Business / Analytics Views)
```

### 🥉 Bronze Layer – Raw Data

* Stores data exactly as received from source systems
* Minimal transformation
* Uses `BULK INSERT` for fast ingestion
* Tables are truncated and reloaded per batch

**Schemas & Tables**

* `bronze.crm_cust_info`
* `bronze.crm_prd_info`
* `bronze.crm_sales_details`
* `bronze.erp_loc_a101`
* `bronze.erp_cust_az12`
* `bronze.erp_px_cat_g1v2`

Load procedure:

* `bronze.load_bronze`

---

### 🥈 Silver Layer – Cleansed & Conformed Data

* Applies business rules and data standardization
* Fixes data quality issues (NULLs, invalid dates, incorrect values)
* Deduplicates records
* Aligns CRM and ERP entities

**Key Transformations**

* Gender & marital status normalization
* Product category extraction
* Date validation & conversion
* Sales amount recalculation
* Country code standardization
* Latest-record selection using window functions

**Schemas & Tables**

* `silver.crm_cust_info`
* `silver.crm_prd_info`
* `silver.crm_sales_details`
* `silver.erp_loc_a101`
* `silver.erp_cust_az12`
* `silver.erp_px_cat_g1v2`

Load procedure:

* `silver.load_silver`

---

### 🥇 Gold Layer – Business & Analytics Layer

* Implements **star schema modeling**
* Creates business-friendly dimensions and facts
* Optimized for BI tools and reporting

**Views**

* `gold.dim_customers`
* `gold.dim_products`
* `gold.fact_sales`

**Design Principles**

* Surrogate keys using `ROW_NUMBER()`
* Conformed dimensions across systems
* Active product filtering
* Fact-to-dimension joins via business keys
* Implemented as views for real-time freshness

---

## Technologies Used

* Microsoft SQL Server
* T-SQL (Stored Procedures, Views, Window Functions)
* ELT Pattern
* Kimball Dimensional Modeling

---

## How to Run the Project

### 1️⃣ Initialize Database & Schemas

```sql
Create database
Create schemas: bronze, silver, gold
```

### 2️⃣ Create Bronze Tables

```sql
Run: Create_Bronze_Tables.sql
```

### 3️⃣ Load Bronze Data

```sql
EXEC bronze.load_bronze;
```

### 4️⃣ Create Silver Tables

```sql
Run: Create_Silver_Tables.sql
```

### 5️⃣ Load Silver Data

```sql
EXEC silver.load_silver;
```

### 6️⃣ Create Gold Views

```sql
Run: Create_Gold_Views.sql
```

---

## Project Highlights

* ✔ Real-world ELT pipeline design
* ✔ Strong data quality handling
* ✔ Clear separation of concerns per layer
* ✔ Interview-ready SQL patterns
* ✔ BI-friendly dimensional model

---

## Future Enhancements

* Incremental loads (CDC)
* Audit & logging tables
* Data quality metrics
* Slowly Changing Dimensions (SCD Type 2)
* Materialized Gold tables
* Power BI / Tableau dashboards

---

## Author

**Jiraphat Sabutr**
Data Engineering & Analytics Enthusiast

---

⭐ If you’re reviewing this project: this warehouse is designed to scale, explain, and extend—exactly how modern analytics systems are built.
