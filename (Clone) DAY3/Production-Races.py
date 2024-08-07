# Databricks notebook source
# MAGIC %run "/Workspace/Users/stephenplathottam@outlook.com/DAY3 Workspace/Include 2024-07-29 16:43:50"

# COMMAND ----------

# DBTITLE 1,READ
df = spark.read.csv(f"{input_path}races.csv", header=True, inferSchema=True)

# COMMAND ----------

# DBTITLE 1,TRANSFORM
df1= df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("path",input_file_name())\
.drop("url")
df1.display()

# COMMAND ----------

# DBTITLE 1,WRITE
df1.write.mode('overwrite').saveAsTable(f"{catalog}.{schema}.races")

# COMMAND ----------

df1.write.mode('overwrite').saveAsTable("spdatabricksws.formula1.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spdatabricksws.formula1.races
