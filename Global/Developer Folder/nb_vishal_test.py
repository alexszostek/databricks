# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

src_path = '/mnt/raw/drivly/Listings/Processing
table_name = 'bronze.drivly_listings'


#if len(dbutils.fs.ls(src_path)) > 0:

 #  df = spark.read.format('org.apache.spark.sql.json')\
  #  .load(src_path)
  # exploddf = df.select(explode("listings"))
   #exploddf.select("col.*").display()

   ##exploddf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

#else:
 #  dbutils.notebook.exit("No Files in Processing folder")

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE bronze.drivly_listings
