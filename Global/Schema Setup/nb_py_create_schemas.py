# Databricks notebook source
s3_path = "s3://rm-databricks-poc-data-bucket/"

schema_json = [
    {"schema_name": "00_raw", "s3_path": "{s3_path}/raw_build/"},
    {"schema_name": "01_bronze", "s3_path": "{s3_path}/bronze_build/"},
    {"schema_name": "02_silver", "s3_path": "{s3_path}/silver_build/"},
    {"schema_name": "03_gold", "s3_path": "{s3_path}/gold_build/"},
    {"schema_name": "04_platinum", "s3_path": "{s3_path}/platinum_build/"},
    {"schema_name": "05_diamond", "s3_path": "{s3_path}/diamon_build/"},
    {"schema_name": "99_reporting", "s3_path": "{s3_path}/reporting_build/"},
    {"schema_name": "common", "s3_path": "{s3_path}/common_build/"}
]

display(schema_json)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.00_raw MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_raw/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.01_bronze MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/bronze_silver';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.02_silver MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_silver/';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.03_gold MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_gold/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.04_platinum MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_platinum/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.05_diamond MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_diamond/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.99_reporting MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_reporting/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.area51 MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_area51/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS build_rocket_mortgage_catalog.common MANAGED LOCATION 's3://rm-databricks-poc-data-bucket/build_common/';
