# Databricks notebook source
input="dbfs:/mnt/hexawaredatabricks/raw/input_files/"
output1="dbfs:/mnt/hexawaredatabricks/raw/output_files/"
output2="dbfs:/mnt/hexawaredatabricks/raw/output_files/"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

def add_ingestion(df):
    b=df.withColumn("ingestion_date",current_timestamp())
    return b

# COMMAND ----------

output="dbfs:/mnt/hexawaredatabricks/raw/output_files/"
