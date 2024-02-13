# Databricks notebook source
df = spark.read.parquet(
#     "abfss://test@rabetads01eus2adlssa.dfs.core.windows.net/Lead_202302132220840-9d4a236c-b523-4da9-b792-7112d7241e3c.parquet"
"/mnt/Bronze/Lead/"
)

# Create a Delta Lake table from the DataFrame
df.write.format("delta").mode("overwrite").save("/mnt/Bronze/lead/delta")

# Read the Delta Lake table into a DataFrame
delta_df = spark.read.format("delta").load("/mnt/Bronze/lead/delta")

# Show the data in the Delta Lake table
display(delta_df)


# COMMAND ----------

## Provide mount with directory where the files exists
mount_path = '/mnt/Bronze/Lead/Queued/'

spark.sql(f"create table IF NOT EXISTS bronze.rasf_lead_staging using DELTA location '{mount_path}' options(header 'true', sep '|')")

## run a group by command on registered table
resultdf  = spark.sql("select Count(*) from bronze.rasf_lead_staging")
resultdf.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table bronze.rasf_lead_staging

# COMMAND ----------


## Provide mount with directory where the files exists
mount_path = 'mnt/bronze/lead/delta'

spark.sql(f"create table IF NOT EXISTS bronze.rasf_lead_staging using DELTA location '{mount_path}' options(header 'true', sep '|')")

## run a group by command on registered table
resultdf  = spark.sql("select Count(*) from bronze.rasf_lead_staging")
resultdf.display()

# COMMAND ----------

# Mount the Azure Data Lake Gen 2 - update 10-21-22 - 1341

dbutils.fs.mount( 
    source = "wasbs://raw@rcnptcusdsdl2.blob.core.windows.net/",
    mount_point = "/mnt",
    extra_configs = {"fs.azure.account.key.rcnptcusdsdl2.blob.core.windows.net":"nscaeBMvUavBDCqEolhli2+1Kpy1nRXxazLNKpBYZfurrpKtqs4WY+o4yMHBajE+ojJadbZhzH/Ce6X6WH71cw=="}
    )

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls('/mnt/')

# COMMAND ----------

""" %python
dbutils.fs.mount(source = 'wasbs://raw@rcnptcusdsdl2.blob.core.windows.net',
                 mount_point = '/mnt/lh',
                extra_configs = {'fs.azure.account.key.rcnptcusdsdl2.blob.core.windows.net' : 'nscaeBMvUavBDCqEolhli2+1Kpy1nRXxazLNKpBYZfurrpKtqs4WY+o4yMHBajE+ojJadbZhzH/Ce6X6WH71cw=='} ) """

# COMMAND ----------

# Mount the Azure Data Lake Gen 2 - update 10-21-22 - 1341

dbutils.fs.mount( 
    source = "wasbs://raw@rabetads01eus2adlssa.dfs.core.windows.net/",
    mount_point = "/mnt",
    extra_configs = {"fs.azure.account.key.rabetads01eus2adlssa.blob.core.windows.net":"/+jOU7bxUy1YFh908RBwU1LE14lq7sdxjGUAmIDzhi3Cikex3Tv5xlb1/a6djbrooOIS9lcERk2f+AStZfu5Gg=="}
    )

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls('/mnt/')

# COMMAND ----------

# """ %python
# dbutils.fs.mount(source = 'wasbs://raw@rcnptcusdsdl2.blob.core.windows.net',
#                  mount_point = '/mnt/lh',
#                 extra_configs = {'fs.azure.account.key.rcnptcusdsdl2.blob.core.windows.net' : 'nscaeBMvUavBDCqEolhli2+1Kpy1nRXxazLNKpBYZfurrpKtqs4WY+o4yMHBajE+ojJadbZhzH/Ce6X6WH71cw=='} ) """

# COMMAND ----------

dbutils.fs.mount(source = "abfss://raw@rabetads01eus2adlssa.dfs.core.windows.net/"
                , mount_point = "/mnt",)


# COMMAND ----------

df.write.option("path", "abfss://raw@rabetads01eus2adlssa.dfs.core.windows.net/Bronze/Lead/delta").saveAsTable("bronze.RocketAutoSalesforceLead")

spark.sql("""
  CREATE TABLE bronze.RocketAutoSalesforceLead
  LOCATION "abfss://raw@rabetads01eus2adlssa.dfs.core.windows.net/Bronze/Lead/delta"
  AS (SELECT *
    FROM parquet.`abfss://raw@rabetads01eus2adlssa.dfs.core.windows.net/Bronze/Lead/Queued`)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table Bronze.RocketAutoSalesforceLead

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from Bronze.RocketAutoSalesforceLead

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/bronze")

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

dbutils.fs.rm("/bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP SCHEMA IF EXISTS bronze CASCADE;
# MAGIC CREATE SCHEMA bronze
# MAGIC LOCATION '/mnt/bronze/delta'
# MAGIC COMMENT 'Bronze Layer is used only for raw source data ingestion';

# COMMAND ----------

DROP SCHEMA IF EXISTS bronze CASCADE;
CREATE SCHEMA bronze
LOCATION '/mnt/Bronze/delta'
COMMENT 'Bronze Layer is used only for raw source data ingestion';


# COMMAND ----------



# COMMAND ----------


