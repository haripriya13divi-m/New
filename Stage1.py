# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import col, desc

spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
df = spark.read.csv('/Workspace/Users/haripriya13divi@gmail.com/New File 2026-06-21 16_10_19.csv', header=True, inferSchema=True)
# df.show()
# df.printSchema()

df.groupBy("dept_id").max("salary").show()

# COMMAND -------

df.filter(df.salary>60000).show()

# COMMAND ---------

df.filter (df.experience>5).show()

# COMMAND ----------

df.groupBy("dept_id").sum('salary').show()


# COMMAND ----------

df.select("dept_id","name","emp_id","salary").filter(col("salary")>60000).show()

# COMMAND ----------

df.withColumn("Annual_Salary",col("salary")*12).show()

# COMMAND ----------


# df.orderBy(desc("salary")).limit(3).show()
# df.filter(col("experience")>5).show()
df.filter(col("dept_id")==10).show()

# COMMAND ----------

# df.show()
df.groupBy("emp_id").count().filter(col("count")>1).show()

# COMMAND ----------

from pyspark.sql.functions import avg, col

df.groupBy("dept_id").agg(avg("salary")).filter(col("avg(salary)")>50000).orderBy(avg("salary").asc()).show()

# COMMAND ----------

df.withColumn("Bonus", col("salary")+1000).filter(col("Bonus")>50000).orderBy(col("Bonus").asc()).limit(5).show()

# COMMAND ----------

from pyspark.sql.functions import col, when

dfw =df.withColumn("Grade", when(col("salary")>70000,"A").when(col("salary")>50000,"B").when(col("salary")>20000,"C").otherwise("D"))
dfw.show()

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.functions import col
from pyspark.sql.functions import sum, avg, max, min, countDistinct, count

df.groupBy("dept_id").agg(sum("salary").alias("total_salary")).show()
df.select(avg("salary")).alias("avg_salary").show()
df.select(count("*")).show()

df.count()

# COMMAND ----------

print(df.schema)
