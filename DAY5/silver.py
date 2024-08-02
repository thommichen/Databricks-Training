# Databricks notebook source
# MAGIC %sql
# MAGIC create schema  if not exists spdatabricksws.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spdatabricksws.bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,industry,company from spdatabricksws.bronze.richest_bronze

# COMMAND ----------

# DBTITLE 1,In pyspark way
df=spark.read.table("spdatabricksws.bronze.richest_bronze")

# COMMAND ----------

# DBTITLE 1,writing to silver table
# MAGIC %sql
# MAGIC Create or replace table silver.richest_silver as
# MAGIC select name, country, industry, net_worth_in_billions, company from bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spdatabricksws.silver.richest_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists spdatabricksws.gold;
# MAGIC use gold

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table gold.country_gold
# MAGIC select country,count(country) count from spdatabricksws.silver.richest_silver group by country order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.country_gold
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.richest_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table gold.name_gold as
# MAGIC select name,count(country) count from spdatabricksws.silver.richest_silver group by name order by count desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.name_gold
