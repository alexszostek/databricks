# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Moduler ETL Process

# COMMAND ----------

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
# MAGIC ### widget (parameter) code

# COMMAND ----------

dbutils.widgets.text("src","");
dbutils.widgets.text("trg", "");
dbutils.widgets.text("primery_key_1", "");
dbutils.widgets.text("dup_remove_key_1", "");

src = 'bronze.'+dbutils.widgets.get("src")
trg_table = 'silver.'+dbutils.widgets.get("trg")
dup_remove_key = dbutils.widgets.get("dup_remove_key_1")
primery_key = dbutils.widgets.get("primery_key_1")

df_read = spark.sql(f"SELECT * FROM {src}")
print(trg_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trg table create if not exists DDL

# COMMAND ----------

table_list=spark.sql("""show tables in silver""")
table_name=table_list.filter(table_list.tableName== dbutils.widgets.get("trg")).collect()
if len(table_name)>0:
    print("table found")
else: 
      df_table = df_read.select([column for column in df_read.columns if "tz__" not in column])
      col_list=[]
      for i in df_table.columns:
          col_list.append(i)
      df_table_final = df_table.withColumn("RecordChecksumNumber", md5(concat_ws("", *col_list)))\
                               .withColumn("ETLInsertBatchID",lit('-1'))\
                               .withColumn("RecordInsertDateTime",lit(datetime.now()))\
                               .withColumn("RecordInsertUserName",lit("Azure_Dbrics_User"))\
                               .withColumn("RecordUpdateDateTime",lit(datetime.now()))\
                               .withColumn("ETLUpdateBatchID",lit('-1'))\
                               .withColumn("RecordUpdateUserName",lit("Azure_Dbrics_User"))
      df_table_final.write.format("delta").saveAsTable(trg_table)
      dbutils.notebook.exit('done')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe for insert and update

# COMMAND ----------

df_read = spark.sql(f"SELECT * FROM {src}")
df_trg = spark.sql(f"SELECT * FROM {trg_table}")
get_rownumber= Window.partitionBy(primary_key).orderBy(dup_remove_key)
df_dist=df_read.withColumn("row_number",row_number().over(get_rownumber)).filter(col("row_number")==1)
df_filter_column = df_dist.select([column for column in df_dist.columns if "tz__" not in column and "row_number" not in column ])


# COMMAND ----------

col_list=[]
for i in df_filter_column.columns:
    col_list.append(i)

##print (col_list)

df_final = df_filter_column.withColumn("RecordChecksumNumber", md5(concat_ws("", *col_list)))\
                           .withColumn("RecordInsertDateTime",lit(datetime.now()))\
                           .withColumn("RecordUpdateDateTime",lit(datetime.now()))


df_final_insert = df_final.select([column for column in df_final.columns if "RecordUpdateDateTime" not in column ])
df_final_insert.createOrReplaceTempView("vw_src_Insert")

df_final_update =  df_final.select([column for column in df_final.columns if "RecordInsertDateTime" not in column ])
df_final_update.createOrReplaceTempView("vw_src_update")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Table DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO silver.$trg AS trg
# MAGIC USING vw_src_update AS src
# MAGIC ON trg.$primary_key_1 = src.$primary_key_1
# MAGIC WHEN MATCHED AND src.RecordChecksumNumber<>trg.RecordChecksumNumber
# MAGIC THEN UPDATE SET * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert Table DDL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.$trg AS trg
# MAGIC USING vw_src_Insert AS src
# MAGIC ON trg.$primary_key_1 = src.$primary_key_1
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver.$trg
# MAGIC ZORDER BY ($primary_key_1)
