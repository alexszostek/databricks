-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC
-- MAGIC This script defines two SQL functions in the `rocket_mortgage_catalog.common` schema:
-- MAGIC
-- MAGIC 1. **udf_field_level_mask_string**: 
-- MAGIC    - Takes a `STRING` input.
-- MAGIC    - Returns the input string if the user belongs to one of the specified account groups (`UG-RocketMortgage-Data-Core-L1`, `L2`, `L3`, `L4`).
-- MAGIC    - Returns `'xxxx'` otherwise.
-- MAGIC
-- MAGIC 2. **udf_field_level_mask_double**:
-- MAGIC    - Takes a `DOUBLE` input.
-- MAGIC    - Returns the input double if the user belongs to one of the specified account groups (`UG-RocketMortgage-Data-Core-L1`, `L2`, `L3`, `L4`).
-- MAGIC    - Returns `-1.0` otherwise.

-- COMMAND ----------

-- drop function if exists `rm-databricks-poc-catalog-2997204634952446`.common.udf_field_level_mask_string;
CREATE
OR REPLACE FUNCTION rocket_mortgage_catalog.common.udf_field_level_mask_string(input_param STRING) RETURN CASE
  WHEN is_account_group_member('UG-RocketMortgage-Data-Core-L1')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L2')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L3')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L4') THEN input_param
  else 'xxxx'
END;

-- COMMAND ----------

-- drop function if exists `rm-databricks-poc-catalog-2997204634952446`.common.udf_field_level_mask_string;
CREATE
OR REPLACE FUNCTION rocket_mortgage_catalog.common.udf_field_level_mask_double(input_param DOUBLE) RETURN CASE
  WHEN is_account_group_member('UG-RocketMortgage-Data-Core-L1')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L2')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L3')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L4') THEN input_param
  else -1.0
END;

-- COMMAND ----------

-- drop function if exists `rm-databricks-poc-catalog-2997204634952446`.common.udf_field_level_mask_string;
CREATE
OR REPLACE FUNCTION rocket_mortgage_catalog.common.udf_field_level_mask_timestamp(input_param TIMESTAMP) RETURN CASE
  WHEN is_account_group_member('UG-RocketMortgage-Data-Core-L1')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L2')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L3')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L4') THEN input_param
  else '1900-01-01 00:00:00'
END;

-- COMMAND ----------

-- drop function if exists `rm-databricks-poc-catalog-2997204634952446`.common.udf_field_level_mask_string;
CREATE
OR REPLACE FUNCTION rocket_mortgage_catalog.common.udf_field_level_mask_integer(input_param INT) RETURN CASE
  WHEN is_account_group_member('UG-RocketMortgage-Data-Core-L1')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L2')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L3')
  or is_account_group_member('UG-RocketMortgage-Data-Core-L4') THEN input_param
  else 0
END;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sql_command = """
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   `rocket_mortgage_catalog`.information_schema.columns c
-- MAGIC where
-- MAGIC   data_type = 'STRING'
-- MAGIC   AND table_schema in (
-- MAGIC     '02_silver',
-- MAGIC     '03_gold',
-- MAGIC     '05_diamond',
-- MAGIC     '04_platinum',
-- MAGIC     '01_bronze'
-- MAGIC   )
-- MAGIC   and not exists (
-- MAGIC     select
-- MAGIC       *
-- MAGIC     from
-- MAGIC       information_schema.column_masks m
-- MAGIC     where
-- MAGIC       m.table_catalog = c.table_catalog
-- MAGIC       and m.table_schema = c.table_schema
-- MAGIC       and m.table_name = c.table_name
-- MAGIC       and m.column_name = c.column_name
-- MAGIC   )
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(sql_command)
-- MAGIC spark.sql(sql_command).createOrReplaceTempView("vw_string_columns")
-- MAGIC
-- MAGIC for row in df.collect():
-- MAGIC     statement = (
-- MAGIC         "alter table `rocket_mortgage_catalog`."
-- MAGIC         + row.table_schema
-- MAGIC         + "."
-- MAGIC         + row.table_name
-- MAGIC         + " ALTER COLUMN "
-- MAGIC         + row.column_name
-- MAGIC         + " SET MASK `rocket_mortgage_catalog`.common.udf_field_level_mask_string"
-- MAGIC     )
-- MAGIC     spark.sql(statement)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sql_command = """
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   `rocket_mortgage_catalog`.information_schema.columns c
-- MAGIC where
-- MAGIC   data_type = 'INT'
-- MAGIC   AND table_schema in (
-- MAGIC     '02_silver',
-- MAGIC     '03_gold',
-- MAGIC     '05_diamond',
-- MAGIC     '04_platinum',
-- MAGIC     '01_bronze'
-- MAGIC   )
-- MAGIC   and not exists (
-- MAGIC     select
-- MAGIC       *
-- MAGIC     from
-- MAGIC       information_schema.column_masks m
-- MAGIC     where
-- MAGIC       m.table_catalog = c.table_catalog
-- MAGIC       and m.table_schema = c.table_schema
-- MAGIC       and m.table_name = c.table_name
-- MAGIC       and m.column_name = c.column_name
-- MAGIC   )
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(sql_command)
-- MAGIC
-- MAGIC for row in df.collect():
-- MAGIC     statement = (
-- MAGIC         "alter table `rocket_mortgage_catalog`."
-- MAGIC         + row.table_schema
-- MAGIC         + "."
-- MAGIC         + row.table_name
-- MAGIC         + " ALTER COLUMN "
-- MAGIC         + row.column_name
-- MAGIC         + " SET MASK `rocket_mortgage_catalog`.common.udf_field_level_mask_integer"
-- MAGIC     )
-- MAGIC     spark.sql(statement)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sql_command = """
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   `rocket_mortgage_catalog`.information_schema.columns c
-- MAGIC where
-- MAGIC   data_type = 'TIMESTAMP'
-- MAGIC   AND table_schema in (
-- MAGIC     '02_silver',
-- MAGIC     '03_gold',
-- MAGIC     '05_diamond',
-- MAGIC     '04_platinum',
-- MAGIC     '01_bronze'
-- MAGIC   )
-- MAGIC   and not exists (
-- MAGIC     select
-- MAGIC       *
-- MAGIC     from
-- MAGIC       information_schema.column_masks m
-- MAGIC     where
-- MAGIC       m.table_catalog = c.table_catalog
-- MAGIC       and m.table_schema = c.table_schema
-- MAGIC       and m.table_name = c.table_name
-- MAGIC       and m.column_name = c.column_name
-- MAGIC   )
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(sql_command)
-- MAGIC
-- MAGIC for row in df.collect():
-- MAGIC     statement = (
-- MAGIC         "alter table `rocket_mortgage_catalog`."
-- MAGIC         + row.table_schema
-- MAGIC         + "."
-- MAGIC         + row.table_name
-- MAGIC         + " ALTER COLUMN "
-- MAGIC         + row.column_name
-- MAGIC         + " SET MASK `rocket_mortgage_catalog`.common.udf_field_level_mask_timestamp"
-- MAGIC     )
-- MAGIC     spark.sql(statement)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sql_command = """
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   `rocket_mortgage_catalog`.information_schema.columns c
-- MAGIC where
-- MAGIC   data_type = 'DOUBLE'
-- MAGIC   AND table_schema in (
-- MAGIC     '02_silver',
-- MAGIC     '03_gold',
-- MAGIC     '05_diamond',
-- MAGIC     '04_platinum',
-- MAGIC     '01_bronze'
-- MAGIC   )
-- MAGIC   and not exists (
-- MAGIC     select
-- MAGIC       *
-- MAGIC     from
-- MAGIC       information_schema.column_masks m
-- MAGIC     where
-- MAGIC       m.table_catalog = c.table_catalog
-- MAGIC       and m.table_schema = c.table_schema
-- MAGIC       and m.table_name = c.table_name
-- MAGIC       and m.column_name = c.column_name
-- MAGIC   )
-- MAGIC """
-- MAGIC
-- MAGIC df = spark.sql(sql_command)
-- MAGIC
-- MAGIC for row in df.collect():
-- MAGIC     statement = (
-- MAGIC         "alter table `rocket_mortgage_catalog`."
-- MAGIC         + row.table_schema
-- MAGIC         + "."
-- MAGIC         + row.table_name
-- MAGIC         + " ALTER COLUMN "
-- MAGIC         + row.column_name
-- MAGIC         + " SET MASK `rocket_mortgage_catalog`.common.udf_field_level_mask_double"
-- MAGIC     )
-- MAGIC     spark.sql(statement)
