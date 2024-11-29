-- Databricks notebook source
-- DROP SCHEMA IF EXISTS bronze;
-- --LOCATION
-- CREATE SCHEMA bronze COMMENT 'Bronze Layer is used only for raw source data ingestion';

DROP SCHEMA IF EXISTS bronze CASCADE;
CREATE SCHEMA bronze 
LOCATION '/mnt/bronze'
COMMENT 'Bronze Layer is used only for raw source data ingestion';

-- COMMAND ----------

DROP SCHEMA IF EXISTS silver CASCADE;
CREATE SCHEMA silver 
LOCATION '/mnt/silver'
COMMENT 'Silver Layer is used only for cleaned/normalized data ingested from bronze layer';

-- COMMAND ----------

DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold 
LOCATION '/mnt/gold'
COMMENT 'Gold Layer is used for aggregated consumer ready data ingested from silver layer';

-- COMMAND ----------

DROP SCHEMA IF EXISTS auditing CASCADE;
CREATE SCHEMA auditing 
LOCATION '/mnt/raw/Auditing'
COMMENT 'Auditing Layer is used for store data engineering meta data and logs';

-- COMMAND ----------


