-- Databricks notebook source
-- MAGIC %sql
-- MAGIC -- DROP SCHEMA IF EXISTS bronze;
-- MAGIC -- --LOCATION
-- MAGIC -- CREATE SCHEMA bronze COMMENT 'Bronze Layer is used only for raw source data ingestion';
-- MAGIC 
-- MAGIC DROP SCHEMA IF EXISTS bronze CASCADE;
-- MAGIC CREATE SCHEMA bronze 
-- MAGIC LOCATION '/mnt/bronze'
-- MAGIC COMMENT 'Bronze Layer is used only for raw source data ingestion';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP SCHEMA IF EXISTS silver CASCADE;
-- MAGIC CREATE SCHEMA silver 
-- MAGIC LOCATION '/mnt/silver'
-- MAGIC COMMENT 'Silver Layer is used only for cleaned/normalized data ingested from bronze layer';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP SCHEMA IF EXISTS gold CASCADE;
-- MAGIC CREATE SCHEMA gold 
-- MAGIC LOCATION '/mnt/gold'
-- MAGIC COMMENT 'Gold Layer is used for aggregated consumer ready data ingested from silver layer';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP SCHEMA IF EXISTS auditing CASCADE;
-- MAGIC CREATE SCHEMA auditing 
-- MAGIC LOCATION '/mnt/raw/Auditing'
-- MAGIC COMMENT 'Auditing Layer is used for store data engineering meta data and logs';

-- COMMAND ----------


