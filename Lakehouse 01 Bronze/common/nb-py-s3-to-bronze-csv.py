# Databricks notebook source
# MAGIC %md
# MAGIC # File Processing and Table Management
# MAGIC
# MAGIC ## Overview
# MAGIC This script performs several tasks:
# MAGIC 1. Moves files from a source directory to a destination directory.
# MAGIC 2. Loads the files into a Spark DataFrame.
# MAGIC 3. Adds metadata columns to the DataFrame.
# MAGIC 4. Checks if a specified table exists, creating it if necessary.
# MAGIC 5. Handles schema mismatches between the DataFrame and the table.
# MAGIC 6. Inserts the data into the table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Code Breakdown
# MAGIC
# MAGIC ### 1. Define Source and Destination Paths
# MAGIC ```python
# MAGIC src_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/queue/"
# MAGIC dest_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/processing/"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Import Necessary Libraries and Set Up Widgets
# MAGIC
# MAGIC In this section, we import the required libraries and set up widgets for user input. The widgets allow users to specify the source system, source object, and whether to skip certain steps in the process.
# MAGIC
# MAGIC

# COMMAND ----------

import os
from pyspark.sql.functions import input_file_name, lit, when
from pyspark.sql.types import TimestampType
from datetime import datetime
from pyspark.sql.functions import md5, concat_ws

# COMMAND ----------


dbutils.widgets.removeAll()
dbutils.widgets.text("source_system", "", "Source System")
dbutils.widgets.text("source_object", "", "Source Object")
dbutils.widgets.dropdown("skip_ind", "false", ["true", "false"], "Skip Indicator")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Script Description
# MAGIC
# MAGIC This script is designed to process CSV files from a specified source system and object, and load them into a bronze layer table in a data catalog. The script performs the following steps:
# MAGIC
# MAGIC 1. **Import Libraries**: Imports necessary libraries including `os`, `pyspark.sql.functions`, `pyspark.sql.types`, and `datetime`.
# MAGIC
# MAGIC 2. **Widget Setup**: Sets up Databricks widgets to capture user inputs for `source_system`, `source_object`, and `skip_ind`.
# MAGIC
# MAGIC 3. **Source Catalog Definition**: Defines the source catalog as `rocket_mortgage_catalog`.
# MAGIC
# MAGIC 4. **Retrieve Widget Values**: Retrieves the values of `source_system` and `source_object` from the widgets.
# MAGIC
# MAGIC 5. **Source Path Definition**: Constructs the source path for the CSV files based on the source catalog, system, and object.
# MAGIC
# MAGIC 6. **Read CSV Files**: Reads the CSV files from the source path into a Spark DataFrame with headers and inferred schema.
# MAGIC
# MAGIC 7. **Table Name Definition**: Defines the table name for the bronze layer using the source catalog, system, and object.
# MAGIC
# MAGIC 8. **Utility Functions**:
# MAGIC    - `get_file_count(input_path)`: Returns the count of files in a given directory.
# MAGIC    - `get_modification_time(file_path)`: Returns the modification time of a file.
# MAGIC
# MAGIC The script is structured to facilitate the processing and loading of data from raw CSV files into a structured table format for further analysis and processing.

# COMMAND ----------

# Define the source catalog
source_catalog = 'rocket_mortgage_catalog'

# Get the source object and system from widgets
source_object = dbutils.widgets.get("source_object")
source_system = dbutils.widgets.get("source_system")

# Define the table name for the bronze layer
table_name = source_catalog + '.01_bronze.' + source_system + '_' + source_object

# COMMAND ----------

# Function to get the count of files in a given directory
def get_file_count(input_path):
    try:
        file_list = dbutils.fs.ls(input_path)
        return len(file_list)
    except Exception as e:
        raise FileNotFoundError(f"No such file or directory: {input_path}")

# Function to get the modification time of a file
def get_modification_time(file_path):
    file_info = dbutils.fs.ls(file_path)
    modification_time = file_info[0].modificationTime
    return datetime.utcfromtimestamp(modification_time / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')

# COMMAND ----------

# MAGIC %md
# MAGIC ### File Processing and Data Ingestion
# MAGIC
# MAGIC This cell performs the following tasks:
# MAGIC 1. Defines source and destination paths for file processing.
# MAGIC 2. Checks if the source path exists and retrieves the list of files.
# MAGIC 3. Moves files from the source to the destination directory.
# MAGIC 4. Reads the files into a Spark DataFrame and adds metadata columns.
# MAGIC 5. Checks if the target table exists in the database.
# MAGIC 6. If the table exists, it ensures the schema matches the DataFrame and adds any new columns.
# MAGIC 7. Inserts the data from the DataFrame into the target table.
# MAGIC
# MAGIC Key functions used:
# MAGIC - `get_file_count(input_path)`: Returns the count of files in a given directory.
# MAGIC - `get_modification_time(file_path)`: Returns the modification time of a file.
# MAGIC
# MAGIC The cell ensures data integrity by adding checksum and metadata columns, and handles schema evolution by adding new columns to the table if necessary.

# COMMAND ----------

src_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/queue/"
dest_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/processing/"
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Check if the source path exists
try:
    file_list = dbutils.fs.ls(src_path)
except Exception as e:
    raise FileNotFoundError(f"No such file or directory: {src_path}")

# Move files from source to destination
for file in file_list:
    file_name = os.path.basename(file.path)
    dbutils.fs.mv(src_path + '/' + file_name, dest_path + '/' + file_name)

if get_file_count(dest_path) == 0:
    dbutils.notebook.exit("No files found in the directory")

# add filepath
df = spark.read.option("header", "true").option("inferSchema", "true").csv([file.path for file in dbutils.fs.ls(f"/Volumes/rocket_mortgage_catalog/00_raw/ingestion/{source_system}/{source_object}/processing/")])
df = df.withColumn("fileName", df["_metadata.file_path"])

# Add modification time for each file
modification_times = [get_modification_time(file.path) for file in dbutils.fs.ls(f"/Volumes/rocket_mortgage_catalog/00_raw/ingestion/{source_system}/{source_object}/processing/")]
modification_times_df = spark.createDataFrame(zip([file.path for file in dbutils.fs.ls(f"/Volumes/rocket_mortgage_catalog/00_raw/ingestion/{source_system}/{source_object}/processing/")], modification_times), ["file_path", "fileModicationTime"])


# Add MD5 column that checks all incoming fields
df = df.withColumn("recordChecksumNumber", md5(concat_ws("||", *df.columns)))
df = df.join(modification_times_df, df["_metadata.file_path"] == modification_times_df["file_path"], "left").drop("file_path")

# Add default metadata column 
df = df.withColumn("currentRecordInd", lit(1)) \
       .withColumn("recordInsertDateTimeUTC", lit(datetime.now())) \
       .withColumn("recordInsertUserName", lit(current_user))\
       .withColumn("recordUpdateDateTimeUTC", lit(datetime.now()))\
       .withColumn("recordUpdateUserName", lit(current_user)) \
       .withColumn("newRecordInd", lit(1))

df.createOrReplaceTempView("temp_table")

# Check if the table exists
table_exists = False
try:
    spark.sql(f"DESCRIBE TABLE {table_name}")
    table_exists = True
    print("Table exists")
except Exception as e:
    print("Table does not exist. It will be created.")
    sql_command = f"create table {table_name} as select * from temp_table"
    spark.sql(sql_command)

# If the table exists, check for schema mismatch
if table_exists:
    # Get columns from the table
    table_columns = spark.table(table_name).columns
    table_columns_set = set(table_columns)

    # Get columns from the DataFrame
    df_columns = df.columns
    df_columns_set = set(df_columns)

    # Identify new columns to be added
    new_columns = df_columns_set - table_columns_set

    # Alter the table to add new columns
    for column in new_columns:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({column} STRING)")
        print(f"Added column '{column}' to table '{table_name}'")

# Ensure the DataFrame has the same columns as the table
final_columns = spark.table(table_name).columns
for col in final_columns:
    if col not in df.columns:
        df = df.withColumn(col, lit(None))  # Add missing columns with default null values

# Reorder DataFrame columns to match the table schema
df = df.select(*final_columns)

# Insert data into the table
df.createOrReplaceTempView("temp_table")
insert_data_query = f"""
INSERT INTO {table_name}
SELECT {', '.join(final_columns)}
FROM temp_table
"""
spark.sql(insert_data_query)
print("Data inserted successfully")


# COMMAND ----------

# MAGIC %md
# MAGIC # File Processing and Archiving
# MAGIC
# MAGIC ## Overview
# MAGIC This script handles file processing tasks, including:
# MAGIC 1. Checking for files in the `processing` directory.
# MAGIC 2. Moving files to a `processed` directory, organized by year, month, and day.
# MAGIC 3. Exiting the process if no files are found.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Code Breakdown
# MAGIC
# MAGIC ### 1. Define Processing and Processed Paths
# MAGIC ```python
# MAGIC processing_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/processing/"
# MAGIC processed_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/processed/"
# MAGIC

# COMMAND ----------

# DBTITLE 1,Code for Moving Files to Processed Directory
processing_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/processing/"
processed_path = f"/Volumes/{source_catalog}/00_raw/ingestion/{source_system}/{source_object}/processed/"

file_list = dbutils.fs.ls(processing_path)
num_files = len(file_list)

s_year = datetime.now().year
s_month = datetime.now().month
s_day = datetime.now().day
processed_path = f"{processed_path}{s_year}/{s_month}/{s_day}/"

if num_files == 0:
    dbutils.notebook.exit("No files found in the directory")

# Check if the source path exists
try:
    file_list = dbutils.fs.ls(processing_path)
except Exception as e:
    raise FileNotFoundError(f"No such file or directory: {processing_path}")


for file in file_list:
    file_name = os.path.basename(file.path)
    dbutils.fs.mv(processing_path + '/' + file_name, processed_path + file_name)
