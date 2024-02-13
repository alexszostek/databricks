-- Databricks notebook source
-- MAGIC %md
-- MAGIC Pipeline_WatermarkLog Table Create Script

-- COMMAND ----------

Create or replace table auditing.Pipeline_WatermarkLog (
 ProcessLogID bigint GENERATED ALWAYS   AS IDENTITY (START WITH 1000 INCREMENT BY 1)
,ProcessName string
,ProcessStartTime timestamp
,ProcessEndTime timestamp
,ProcessStatus string
,RecordInsertDateTime timestamp 
,RecordUpdateDateTime timestamp 
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Table Clean Up Script

-- COMMAND ----------

--Truncate table auditing.Pipeline_WatermarkLog 
