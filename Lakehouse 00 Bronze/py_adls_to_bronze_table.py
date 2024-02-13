# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC use bronze

# COMMAND ----------

dbutils.widgets.text("src_env", "");
dbutils.widgets.text("src_folder", "");
dbutils.widgets.text("table_name", "");

# COMMAND ----------

src_path = '/mnt/raw/'+dbutils.widgets.get("src_env")+'/'+dbutils.widgets.get("src_folder")+'/Processing'
table_name = 'bronze.'+dbutils.widgets.get("table_name")

if len(dbutils.fs.ls(src_path)) > 0:
    df = spark.read.option("inferschema",True).option("header",True,).option("sep",'|').parquet(src_path)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

else:
    dbutils.notebook.exit("No Files in Processing folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validation

# COMMAND ----------

#print(table_name)
#print(src_path)

