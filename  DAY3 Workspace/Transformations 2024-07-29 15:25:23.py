# Databricks notebook source
df = spark.read.csv("dbfs:/mnt/spadlsstorage/dataset/circuits.csv", header=True, inferSchema=True)
display(df)

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("name","location").show()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("name").alias("new_name"),"location",df["country"])
display(df)

# COMMAND ----------

df.select(concat("country",lit(" "),"location"))
display(df)

# COMMAND ----------

(df.withColumnRenamed("country","Country") 
  .withColumnRenamed("lat","LAT") 
  .display())

# COMMAND ----------

df.withColumnRenamed("country", "Country") \
  .withColumnRenamed("lat", "LAT") \
  .display()

# COMMAND ----------

df.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

(df
.withColumn("ingestion_date",current_date())
.withColumn("path",input_file_name())
.drop("url")
.display())
