/*
===============================================================================
 Script Name : Initialize_DataWarehouse.sql
 Description : 
    This script initializes the DataWarehouse environment by:
      - Checking if the DataWarehouse database already exists
      - Forcing single-user mode and dropping the existing database (if found)
      - Creating a fresh DataWarehouse database
      - Creating data processing schemas: bronze, silver, and gold

 Purpose :
    Designed to reset and prepare a clean Data Warehouse structure following
    the Medallion Architecture pattern:
      - bronze : Raw / ingested data
      - silver : Cleaned and transformed data
      - gold   : Curated, analytics-ready data

 Warning :
    ⚠️ This script will PERMANENTLY DELETE the existing 'DataWarehouse' database.
    Use with caution and ensure backups are taken before execution.

 Created   : 2026-01-28
 Database  : Microsoft SQL Server
===============================================================================
*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE Datwarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Datawarehouse
END;
GO
  
CREATE DATABASE DataWarehouse;
GO
  
USE DataWarehouse;
GO
  
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
