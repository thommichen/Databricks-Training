# Databricks notebook source
# MAGIC %md
# MAGIC ### Demo of Delta Lake change data feed

# COMMAND ----------

# MAGIC %md #### Create a silver table that tracks absolute number vaccinations and available doses by country

# COMMAND ----------

countries = [("USA", 10000, 20000), ("India", 1000, 1500), ("UK", 7000, 10000), ("Canada", 500, 700) ]
columns = ["Country","NumVaccinated","AvailableDoses"]
spark.createDataFrame(data=countries, schema = columns).write.format("delta").mode("overwrite").saveAsTable("silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverTable

# COMMAND ----------

import pyspark.sql.functions as F
spark.read.format("delta").table("silverTable").withColumn("VaccinationRate", F.col("NumVaccinated") / F.col("AvailableDoses")) \
  .drop("NumVaccinated").drop("AvailableDoses") \
  .write.format("delta").mode("overwrite").saveAsTable("goldTable")

# COMMAND ----------

# MAGIC %md #### Generate gold table showing vaccination rate by country

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM goldTable

# COMMAND ----------

##next we will make some changes to silver table and should see that changes at the same time in gold table...means implemnet those changes in gold table

# COMMAND ----------

##next i will make changes in silver table and shud see that changes in gold directly

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable change data feed on silver table

# COMMAND ----------

## it is enabling to cpatuerd the changes (insert,update,delete) made on the table using an inbuit fn called table changes.On all dimension tables where upd,ins,dele happens.Only for delta format..It must be enabled at the beginning..if not done enable it through ALTER command

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silverTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended silverTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update silver table daily

# COMMAND ----------

# Insert new records
new_countries = [("Australia", 100, 3000)]
spark.createDataFrame(data=new_countries, schema = columns).write.format("delta").mode("append").saveAsTable("silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update a record
# MAGIC UPDATE silverTable SET NumVaccinated = '11000' WHERE Country = 'USA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete a record
# MAGIC DELETE from silverTable WHERE Country = 'UK'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history silverTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore the change data in SQL and PySpark 

# COMMAND ----------

## to see all changes on silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history silverTable

# COMMAND ----------

##(2,5)...it shows which all versions we have to display from above command.if we write just 2 it will show all versions from 2 to max.

# COMMAND ----------

# commit timestamp to orderby latest data

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- view the changes
# MAGIC SELECT * FROM table_changes('silverTable', 2,5) order by _commit_timestamp

# COMMAND ----------

##below is the same result with python

# COMMAND ----------

changes_df = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 2).table('silverTable')
display(changes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Propagate changes from silver to gold table

# COMMAND ----------

##for that we need window functions..That is why we first rank it by country and rank it by commit timestamp or co mmit version also.I fthere are 2 inserts of USA ,rank will show 1 and 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect only the latest version for each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silverTable_latest_version as
# MAGIC SELECT * 
# MAGIC     FROM 
# MAGIC          (SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
# MAGIC           FROM table_changes('silverTable', 2, 5)
# MAGIC           WHERE _change_type !='update_preimage')
# MAGIC     WHERE rank=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverTable_latest_version

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the changes to gold
# MAGIC MERGE INTO goldTable t USING silverTable_latest_version s ON s.Country = t.Country
# MAGIC         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
# MAGIC         WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from goldTable

# COMMAND ----------

##change type will only capture the changes and implement it.Delete we will see

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO goldTable t USING silverTable_latest_version s ON s.Country = t.Country
# MAGIC         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
# MAGIC         WHEN MATCHED AND s._change_type='delete' THEN DELETE
# MAGIC         WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM goldTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE silverTable;
# MAGIC DROP TABLE goldTable;
