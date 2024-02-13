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
-- MAGIC # Recored Update Script

-- COMMAND ----------

Update auditing.Pipeline_WatermarkLog
Set
 ProcessStatus = 'Failed'
,ProcessEndTime = current_timestamp()
,RecordUpdateDateTime = current_timestamp()

where ProcessLogID = (select max(ProcessLogID) from auditing.Pipeline_WatermarkLog where ProcessName = '${parameter.ProcessName}' and ProcessStatus = 'Running'  )


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Notebook Exit Script

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.notebook.exit("Success")
