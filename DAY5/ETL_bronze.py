# Databricks notebook source
# MAGIC %run "/Workspace/Users/stephenplathottam@outlook.com/DAY5/Includes"

# COMMAND ----------

#dbutils.help()

# COMMAND ----------

#dbutils.widgets.help()

# COMMAND ----------

#dbutils.widgets.text("")

# COMMAND ----------

input

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

##df.withColumn("ingestion_date",current_timestamp()).display()
##we can automate this usning UDF

# COMMAND ----------

df1=add_ingestion(df)
## when new data comes to DB we create a column called  current timestamp coulm 

# COMMAND ----------

#display(df1)

# COMMAND ----------

df1.columns

# COMMAND ----------

new_col=['name', 'country', 'industry', 'net_worth_in_billions', 'company','ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w=dbutils.widgets.get("environment")
w

# COMMAND ----------

df3=df2.withColumn("environment",lit(w))

# COMMAND ----------

##in above case, one column is extra .so we cannot save it to older file "stephen richest" since no of columns donot match.for that we use schema evalution.For that we use merge schema to add a new coulmn to the file in azure

# COMMAND ----------

###we can save it in files, or tables.yesterday we have seen saveastable to a tabele.Today we will save in a file in azure

# COMMAND ----------

#%fs ls

# COMMAND ----------

#%fs ls dbfs:/mnt/

# COMMAND ----------

#output
#output1
#output2

# COMMAND ----------

#%fs ls 'dbfs:/mnt/hexawaredatabricks/raw/output_files/stephen/richest/_delta_log/'

# COMMAND ----------

#df3.write.mode("overwrite").save(f"{output1}stephen/richest")

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").save(f"{output}stephen/richest")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from delta.`dbfs:/mnt/hexawaredatabricks/raw/output_files/stephen/richest`

# COMMAND ----------

##since we have to use this in prod environment we use overwrite mode

# COMMAND ----------

#df1.createOrReplaceTempView("richest_view")




# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from richest_view

# COMMAND ----------

##why Iam creating view
##bcoz here net worth is having spaces between them ..so while quering that we have to use backtick..Even in pyspark same..So if special characters or spaces are there in coulmns we have to use backtick

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from richest_view where `Net Worth (in billions)` > 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC --select Name as name,Country as country from richest_view
# MAGIC

# COMMAND ----------

#df1.createOrReplaceGlobalTempView("richest_globalview")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from global_temp.richest_globalview

# COMMAND ----------

w

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog spdatabricksws;
# MAGIC create schema if not exists bronze;
# MAGIC use bronze;
# MAGIC
# MAGIC

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").saveAsTable("richest_bronze")

# COMMAND ----------

df3.display

# COMMAND ----------

###here we are writing to a table instead of file.actually we donot need merge schema but we are using it here
