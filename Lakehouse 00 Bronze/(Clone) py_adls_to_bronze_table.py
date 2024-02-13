# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use bronze

# COMMAND ----------

dbutils.widgets.text("src_env", "");
dbutils.widgets.text("src_folder", "");
dbutils.widgets.text("table_name", "");

# COMMAND ----------

# Example JSON configuration
config_json = {
  "tables": [
    {
      "table_name": "sales_data",
      "fields_to_exclude": ["customer_social_security_number", "raw_sales_data"]
    },
    {
      "table_name": "employee_records",
      "fields_to_exclude": ["home_address", "personal_email", "social_security_number"]
    },
    {
      "table_name": "product_inventory",
      "fields_to_exclude": ["supplier_contact_info", "internal_notes"]
    },
    {
      "table_name": "customer_feedback",
      "fields_to_exclude": ["customer_email", "customer_phone"]
    }
  ]
}

def get_fields_to_exclude(table_name):
    """
    Looks up fields to exclude for a given table name.

    Parameters:
    - table_name (str): The name of the table to look up.

    Returns:
    - list[str]: A list of fields to exclude for the given table.
    """
    # Iterate over the tables in the configuration
    for table in config_json['tables']:
        # Check if the current table matches the input table name
        if table['table_name'] == table_name:
            # Return the fields to exclude for this table
            return table['fields_to_exclude']
    
    # If no matching table is found, return an empty list
    return []

# Example usage
table_name = "sales_data"
fields_to_exclude_list = get_fields_to_exclude(table_name)

# Assuming df is your source DataFrame

# Fields to exclude
fields_to_exclude = fields_to_exclude_list

src_path = '/mnt/raw/'+dbutils.widgets.get("src_env")+'/'+dbutils.widgets.get("src_folder")+'/Processing'
table_name = 'bronze.'+dbutils.widgets.get("table_name")

if len(dbutils.fs.ls(src_path)) > 0:
    df = spark.read.option("inferschema",True).option("header",True,).option("sep",'|').parquet(src_path).df_dropped(*fields_to_exclude)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

else:
    dbutils.notebook.exit("No Files in Processing folder")


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

