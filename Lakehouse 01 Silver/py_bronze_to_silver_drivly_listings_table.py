# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook config

# COMMAND ----------

# Importing packages	

import pyspark	
from pyspark.sql import SparkSession	
from pyspark.sql.window import Window	
from pyspark.sql.functions import rank, col	
from pyspark.sql.functions import row_number
from delta import *
from delta.tables import *
from datetime import *
from pyspark.sql.functions import md5, concat_ws
from pyspark.sql.functions import lit

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe for insert

# COMMAND ----------

df_read = spark.sql(f"SELECT * FROM bronze.drivly_listings")
#df_trg = spark.sql(f"SELECT * FROM silver.rocketautodrivlylistings")

col_list=[]
for i in df_read.columns:
    col_list.append(i)

df_rcsn = df_read.withColumn("RecordChecksumNumber", md5(concat_ws("", *col_list)))
get_rownumber= Window.partitionBy("vin").orderBy("RecordChecksumNumber")
df_dist  = df_rcsn.withColumn("row_number",row_number().over(get_rownumber)).filter(col("row_number")==1).withColumn("SnapshotTimestamp",lit(datetime.now()))\
                                                                                                         .withColumn("ETLInsertBatchID",lit('-1'))\
                                                                                                         .withColumn("RecordInsertDateTime",lit(datetime.now()))\
                                                                                                         .withColumn("RecordInsertUserName",lit("Azure_Dbrics_User"))\
                                                                                                         .withColumn("RecordUpdateDateTime",lit(datetime.now()))\
                                                                                                         .withColumn("ETLUpdateBatchID",lit('-1'))\
                                                                                                         .withColumn("RecordUpdateUserName",lit("Azure_Dbrics_User"))
df_final = df_dist.select(['SnapshotTimestamp','accidents', 'askingPrice', 'auction', 'autocheckScore', 'buyNowPrice', 'city', 'comments', 'condition', 'conditionReport', 'date', 'dealerCity', 'dealerState', 'description', 'distance', 'dom', 'doors', 'drivetrain', 'engine', 'exterior', 'facility', 'fuel', 'images', 'interior', 'lane', 'latitude', 'longitude', 'make', 'margin', 'marginpercent', 'mileage', 'minimumBid', 'model', 'owners', 'pickupLocation', 'price', 'primaryImage', 'retailId', 'retailValue', 'runNumber', 'saleDate', 'seats', 'seller', 'state', 'style', 'transmission', 'trim', 'type', 'url', 'vdp', 'vin', 'wholesaleValue', 'year', 'zip', 'RecordChecksumNumber', 'ETLInsertBatchID', 'RecordInsertDateTime', 'RecordInsertUserName','ETLUpdateBatchID', 'RecordUpdateDateTime', 'RecordUpdateUserName'])

# COMMAND ----------

df_final.write.mode('append').insertInto('silver.rocketautodrivlylistings')
