-- Databricks notebook source
CREATE STREAMING TABLE sales_bronze
comment "the raw sales table"
as
select *,current_timestamp() as ingestion FROM cloud_files('dbfs:/mnt/hexawaredatabricks/raw/dlt_input/sales/',"csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

CREATE STREAMING TABLE customers_bronze
comment "the raw customer table"
as
select *,current_timestamp() as ingestion FROM cloud_files('dbfs:/mnt/hexawaredatabricks/raw/dlt_input/customers/',"csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

CREATE STREAMING TABLE product_bronze
comment "the raw product table"
as
select *,current_timestamp() as ingestion FROM cloud_files('dbfs:/mnt/hexawaredatabricks/raw/dlt_input/product/',"csv",map("cloudFiles.inferColumnTypes","True"))
