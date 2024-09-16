# Databricks notebook source
# MAGIC %md
# MAGIC # Create Table from a file on s3
# MAGIC
# MAGIC - The files is coming from our product engineering partner "ourhouse" and is new object called customer
# MAGIC - We get all our raw files in an s3 bucket call raw>ingestion>queue

# COMMAND ----------

df = spark.read.csv("/Volumes/rm-databricks-poc-catalog-2997204634952446/raw/ingestion/ourhouse/customer/processing/eff90e60-8efe-48b0-83cf-d717737fd9d8.csv", header=True, inferSchema=True)
df.write.format("delta").saveAsTable("bronze.table_a")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.table_a

# COMMAND ----------

# MAGIC %md
# MAGIC # Add some sort of code to hide sensitve data from UG-RocketMortgage-Data-Core-L2
# MAGIC
# MAGIC - L2 are rocket data team member that allowed to query data, but not allowed to sensitve data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop function if exists `rm-databricks-poc-catalog-2997204634952446`.common.udf_field_level_mask_string;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION `rm-databricks-poc-catalog-2997204634952446`.common.udf_field_level_mask_string(input_param STRING)
# MAGIC   RETURN CASE WHEN is_account_group_member('UG-RocketMortgage-Data-Core-L2')  THEN input_param else 'xxxx' END;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Now add that same filter to all string fields

# COMMAND ----------

sql_command = """
select
  *
from
  `rm-databricks-poc-catalog-2997204634952446`.information_schema.columns c
where
  data_type = 'STRING'
  AND table_schema in (
    'silver',
    'gold',
    'diamond',
    'platinum',
    'bronze'
  )
  and not exists (
    select
      *
    from
      information_schema.column_masks m
    where
      m.table_catalog = c.table_catalog
      and m.table_schema = c.table_schema
      and m.table_name = c.table_name
      and m.column_name = c.column_name
  )
"""

df = spark.sql(sql_command)
spark.sql(sql_command).createOrReplaceTempView("vw_string_columns")

for row in df.collect():
    statement = (
        "alter table `rm-databricks-poc-catalog-2997204634952446`."
        + row.table_schema
        + "."
        + row.table_name
        + " ALTER COLUMN "
        + row.column_name
        + " SET MASK `rm-databricks-poc-catalog-2997204634952446`.common.udf_field_level_mask_string"
    )
    spark.sql(statement)

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table `rm-databricks-poc-catalog-2997204634952446`.bronze.table_a
# MAGIC alter column c_name drop mask

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table `rm-databricks-poc-catalog-2997204634952446`.bronze.table_a
# MAGIC alter column c_address drop mask

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table `rm-databricks-poc-catalog-2997204634952446`.bronze.table_a
# MAGIC alter column c_nationkey drop mask

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table `rm-databricks-poc-catalog-2997204634952446`.bronze.table_a
# MAGIC alter column c_phone drop mask

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table `rm-databricks-poc-catalog-2997204634952446`.bronze.table_a
# MAGIC alter column c_name drop mask
