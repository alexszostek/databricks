-- Databricks notebook source
CREATE EXTERNAL VOLUME rocket_mortgage_catalog.`00_raw`.archive
    LOCATION 's3://rm-databricks-poc-data-bucket/archive'
    COMMENT 'This is my example external volume on S3'


    -- arn:aws:s3:::rm-databricks-poc-data-bucket

-- COMMAND ----------

select * from rocket_mortgage_catalog.`00_raw`.ingestion
