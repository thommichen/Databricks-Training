# Databricks notebook source
# MAGIC %run "/Workspace/Users/stephenplathottam@outlook.com/DAY5/Includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

str_schema="name string, country String, industry string, net_worth_in_billion double, company string"

# COMMAND ----------

from pyspark.sql.types import *
pyspark_schmema= StructType([StructField("name",StringType()),
                             StructField("country",StringType()),
                             StructField("industry",StringType()),
                             StructField("net_worth",DoubleType()),
                             StructField("company",StringType())
])

# COMMAND ----------

df_new2=spark.read.schema(pyspark_schmema).csv(f"{input}",header=True)
