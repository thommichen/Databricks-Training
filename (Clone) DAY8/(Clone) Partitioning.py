# Databricks notebook source
# MAGIC %run "/Workspace/Users/stephenplathottam@outlook.com/DAY5/Includes"

# COMMAND ----------

input

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

users_schema=StructType([StructField("Year",IntegerType()),
                         StructField("first_name",StringType()),
                         StructField("County",StringType()),
                         StructField("Gender",StringType()),
                         StructField("Count",IntegerType())
])

# COMMAND ----------

df=spark.read.csv(f"{input}Baby_Names.csv",header=True,schema=users_schema)

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().orderBy("Year").display()

# COMMAND ----------

df.write.saveAsTable("spdatabricksws.bronze.baby_name")

# COMMAND ----------

df.write.save("dbfs:/mnt/hexawaredatabricks/raw/output_files/Stephen/baby_names")

# COMMAND ----------

df.write.partitionBy("Year").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/Stephen/baby_names_year")

# COMMAND ----------

df.write.partitionBy("Year","Gender").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/Stephen/baby_names_year_gender")
