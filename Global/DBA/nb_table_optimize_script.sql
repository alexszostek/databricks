-- Databricks notebook source
Optimize gold.DateDim zorder by (DateSK,DayDate);

-- COMMAND ----------

Optimize gold.TimeDim zorder by (TimeSK,Time);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import udf

-- COMMAND ----------

select squaredWithPython(2)

-- COMMAND ----------

optimize auditing.Pipeline_WatermarkLog zorder by (ProcessLogID,ProcessName,ProcessStatus)

-- COMMAND ----------

optimize gold.leadfact zorder by (LeadSK,LeadID,CreateESTDate)
