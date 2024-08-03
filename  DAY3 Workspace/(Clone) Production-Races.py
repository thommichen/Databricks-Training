# Databricks notebook source
df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/spadlsstorage/dataset/races.csv")


# COMMAND ----------

from pyspark.sql.functions import *


 


# COMMAND ----------

df1= df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("path",input_file_name())\
.drop("url")

# COMMAND ----------

df1.write.saveAsTable("spdatabricksws.formula1.races")
