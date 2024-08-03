# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path = "dbfs:/mnt/spadlsstorage/dataset/"

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog spdatabricksws;
# MAGIC use schema formula1
# MAGIC

# COMMAND ----------

catalog='spdatabricksws'
schema='formula1'
