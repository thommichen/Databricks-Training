# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/spadlsstorage/dataset/circuits.csv", header=True, inferSchema=True)

display(df)

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.saveAsTable("spdatabricksws.formula1.circuit")
