-- Databricks notebook source
CREATE STREAMING TABLE sales_bronze 
comment "the raw sales table"
as 
select *,current_timestamp() as ingestion_date FROM cloud_files('dbfs:/mnt/hexawaredatabricks/raw/dlt_input/sales/',"csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sales_silver
(CONSTRAINT order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
as
select distinct(*) from STREAM(LIVE.sales_bronze)

-- COMMAND ----------

CREATE STREAMING TABLE customers_bronze 
comment "the raw customer table"
as 
select *,current_timestamp() as ingestion_date FROM cloud_files('dbfs:/mnt/hexawaredatabricks/raw/dlt_input/customers/',"csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE customers_silver;

APPLY CHANGES INTO
  live.customers_silver
FROM
  stream(live.customers_bronze)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation, sequenceNum,_rescued_data,ingestion_date)
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

CREATE STREAMING TABLE product_bronze 
comment "the raw product table"
as 
select *,current_timestamp() as ingestion_date FROM cloud_files('dbfs:/mnt/hexawaredatabricks/raw/dlt_input/product/',"csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

-- Create and populate the target table.
CREATE OR REFRESH STREAMING TABLE product_silver;

APPLY CHANGES INTO
  live.product_silver
FROM
  stream(live.product_bronze)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation, seqNum,_rescued_data,ingestion_date)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

create live table sales_customer
comment "gold" 
as
select s.customer_id, s.order_id, s.total_amount, c.customer_name, c.customer_email
from LIVE.sales_silver s
inner join live.customers_silver c
on s.customer_id=c.customer_id
