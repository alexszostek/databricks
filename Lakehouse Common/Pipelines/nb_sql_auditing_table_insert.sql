-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Input Paramter Read Script

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("ProcessName", "ProcessNameDefault")
-- MAGIC ProecessName = dbutils.widgets.get("ProcessName")
-- MAGIC spark.conf.set('parameter.ProcessName',ProecessName)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Recored Insert Script

-- COMMAND ----------

Insert into auditing.Pipeline_WatermarkLog
(	
 ProcessName
,ProcessStartTime
,ProcessEndTime
,ProcessStatus
,RecordInsertDateTime
,RecordUpdateDateTime
)
Values
(
 '${parameter.ProcessName}'
 ,current_timestamp()
 ,current_timestamp()
 ,'Running'
 ,current_timestamp()
 ,current_timestamp()
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Notebook Exit Script

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.notebook.exit("Success")
