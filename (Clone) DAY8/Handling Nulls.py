# Databricks notebook source
schema=["name", "subject", "Marks", "Attendance"]
 
student_data=[("John","Math", 90, 80),("Michael", "Science", 70, None), ("David", "History", 50,40), ("Kelvin", "Computer", 40,None ),("Paul", "GEO", None, None), (None,None,  None, None),("John","Math", 90, 80),("John","Math", 90, 80),(None,None,  None, None),(None,None,  None, None),(None,None,  None, None),("Michael", "Science", 70, None),(None, "Science", 90, None),(" ", "NAN", 55, None)]
 
df=spark.createDataFrame(data=student_data, schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

df1=df.distinct().display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

###df.dropDuplicates(["name"]).display()

# COMMAND ----------

#in this context both distinct and drop dulpi are same but if we have to remove duplicates of a specific column we use drop dupli

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

df2.dropna(subset="name")

# COMMAND ----------

df2.fillna({"Marks":39,"Attendance":34})

# COMMAND ----------

df3=df2.fillna(34,subset="Attendance")

# COMMAND ----------

df3.replace("NAN","IT",subset="subject").display()

# COMMAND ----------

df3.replace("NAN","IT",subset="subject").replace(" ","Robert").display()
