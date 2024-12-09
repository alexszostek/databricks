# Databricks notebook source
sql_command = f"""
select
  concat(table_schema, '.', table_name ) as fullTableName
from
  rocket_mortgage_catalog.information_schema.tables
where
  table_schema = '02_silver'
  and table_type = 'MANAGED'
  """

df = spark.sql(sql_command)

df.createOrReplaceTempView("vw_tables")

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC
# MAGIC create view if not exists 99_reporting.vw_tpch_customer
# MAGIC as
# MAGIC select * from 02_silver.tpch_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rocket_mortgage_catalog.information_schema.columns
# MAGIC where 
