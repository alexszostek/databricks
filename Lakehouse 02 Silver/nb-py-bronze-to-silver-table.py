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

dbutils.widgets.removeAll()
dbutils.widgets.text("source_system","")
dbutils.widgets.text("source_object", "")
dbutils.widgets.text("primary_key", "")
dbutils.widgets.text("sort_key", "")

# COMMAND ----------

source_system = dbutils.widgets.get("source_system")
source_object = dbutils.widgets.get("source_object")
primary_key = dbutils.widgets.get("primary_key" )
sort_key = dbutils.widgets.get("sort_key")

# COMMAND ----------


src = 'rocket_mortgage_catalog.01_bronze.' +dbutils.widgets.get("source_system") + "_" + dbutils.widgets.get("source_object")
trg = 'rocket_mortgage_catalog.02_silver.' + dbutils.widgets.get("source_system") + "_" + dbutils.widgets.get("source_object")
sort_key = dbutils.widgets.get("sort_key")
primary_key = dbutils.widgets.get("primary_key")

sql_command = f"""
select * from {src} where newRecordInd = 1
"""

df_read = spark.sql(sql_command)

columns_to_remove = ["currentRecordInd", "recordInsertDateTimeUTC", "recordInsertUserName", "recordUpdateDateTimeUTC", "recordUpdateUserName", "newRecordInd", "etlInsertBatchID"]
df_filtered = df_read.drop(*columns_to_remove)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if Target Table Exists and Create if Not

# COMMAND ----------

# DBTITLE 1,Error Handling and Table Creation with Spark SQL
sql_command = f""" select count(*) from {trg} """
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

try:
    result = spark.sql(sql_command)
    print(f"Query executed successfully. Table Exists")
except Exception as e:
    print(f"An error occurred: {e}")
    df_table = df_filtered.select([column for column in df_read.columns if "tz__" not in column])
    col_list=[]
    for i in df_filtered.columns:
        col_list.append(i)
    df_table_final = df_filtered.withColumn("etlInsertBatchID",lit('-1'))\
                             .withColumn("recordInsertDateTimeUTC",lit(datetime.now()))\
                             .withColumn("recordInsertUserName",lit(current_user))\
                             .withColumn("recordUpdateDateTimeUTC",lit(datetime.now()))\
                             .withColumn("etlUpdateBatchID",lit('-1'))\
                             .withColumn("recordUpdateUserName",lit(current_user)) \
                             .withColumn("currentRecordInd", lit(1))
    df_table_final.createOrReplaceTempView("vw_src")
    sql_command = f"""
        create table {trg} as 
        select * from vw_src
        """
    spark.sql(sql_command)
    update_sql = f"""
        ALTER TABLE {trg}
        ADD COLUMNS (
            etlInsertBatchID STRING,
            recordInsertDateTimeUTC TIMESTAMP,
            recordInsertUserName STRING,
            recordUpdateDateTimeUTC TIMESTAMP,
            etlUpdateBatchID STRING,
            recordUpdateUserName STRING,
            currentRecordInd INT
        )
    """
    spark.sql(update_sql)

    dbutils.notebook.exit("Table Created")

# COMMAND ----------

# DBTITLE 1,Check and Update Table Schema with New Columns
# Check if the table exists
table_exists = True
target_table = '`02_silver`.test_customer'
source_table = '`01_bronze`.test_customer'

# If the table exists, check for schema mismatch
if table_exists:
    # Get columns from the target table
    source_table_columns = spark.table(target_table).columns
    source_table_columns_set = set(source_table_columns)

    # Get columns from the source table, ignoring 'newRecordInd'
    target_table_columns = [col for col in spark.table(source_table).columns if col != 'newRecordInd']
    target_table_columns_set = set(target_table_columns)

    # Identify new columns to be added
    new_columns = target_table_columns_set - source_table_columns_set
    print(new_columns)

    # Alter the table to add new columns
    for column in new_columns:
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({column} STRING)")
        print(f"Added column '{column}' to table '{target_table}'")

# Fields to remove from the merge operation
fields_to_remove = [
    "source.currentRecordInd",
    "source.recordInsertDateTimeUTC",
    "source.recordInsertUserName", 
    "source." + primary_key
]

# Filter out the fields to be removed from the target table columns
target_table_columns = [col for col in target_table_columns if f"source.{col}" not in fields_to_remove]

# Construct the merge SQL query
merge_sql = f"""
WITH deduplicated_source AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {sort_key} DESC) AS row_num
  FROM
    {source_table}
  WHERE
    newRecordInd = 1
)
MERGE INTO {target_table} AS target
USING (
  SELECT
    *
  FROM
    deduplicated_source
  WHERE
    row_num = 1
) AS source
ON target.{primary_key} = source.{primary_key}
AND target.recordChecksumNumber <> source.recordChecksumNumber
WHEN MATCHED THEN
    UPDATE SET
        {', '.join([f'target.{col} = source.{col}' for col in target_table_columns])}
"""

# Execute the merge SQL query
display(spark.sql(merge_sql))

# COMMAND ----------

# Ensure the primary key is included in the target table columns
if primary_key not in target_table_columns:
    target_table_columns.append(primary_key)

# Construct the SQL query for inserting new records
insert_sql = f"""
WITH cte AS (
  SELECT
    {', '.join([f'{col}' for col in target_table_columns])},
    ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {sort_key} DESC) AS row_num
  FROM
    {source_table} source
  WHERE
    NOT EXISTS (
      SELECT
        *
      FROM
        {target_table} target
      WHERE
        target.{primary_key} = source.{primary_key}
    )
    AND source.newRecordInd = 1
)
INSERT INTO {target_table} ({', '.join([f'{col}' for col in target_table_columns])})
SELECT
  {', '.join([f'{col}' for col in target_table_columns])}
FROM
  cte
WHERE
  row_num = 1
"""

# Execute the SQL query and display the result
display(spark.sql(insert_sql))

# COMMAND ----------

sql_command = f"""
update {source_table}
set newRecordInd = 0 
where newRecordInd = 1
"""

spark.sql(sql_command)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `02_silver`.test_customer
