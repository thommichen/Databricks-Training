# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/spadlsstorage/dataset/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/spadlsstorage/dataset/pit_stops.json

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/spadlsstorage/dataset/pit_stops.json",multiLine=True,header=True, inferSchema=True)


# COMMAND ----------

display(df)

# COMMAND ----------

df.write.saveAsTable("spdatabricksws.formula1.pitstop")
