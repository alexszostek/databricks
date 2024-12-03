# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook config

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read/Write procesing folder to drivly bronze table

# COMMAND ----------

src_path = '/mnt/raw/drivly/Listings/Processing'
table_name = 'bronze.drivly_listings'


if len(dbutils.fs.ls(src_path)) > 0:
 df = spark.read.format('org.apache.spark.sql.json')\
    .load(src_path)
 exploddf = df.select(explode("listings"))
 finaldf = exploddf.select("col.*")
 ##finaldf.count()
 finaldf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

else:
 dbutils.notebook.exit("No Files in Processing folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table OPTIMIZE

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE bronze.drivly_listings

# COMMAND ----------


