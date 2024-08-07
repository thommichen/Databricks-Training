# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/spadlsstorage/dataset/drivers.json

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table formula1.drivers as
# MAGIC select * from json.`dbfs:/mnt/spadlsstorage/dataset/drivers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.drivers
