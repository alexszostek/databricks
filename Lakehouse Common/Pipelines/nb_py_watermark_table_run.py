# Databricks notebook source
from datetime import datetime

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("skipInd", "False", ["False", "True"], "Skip Indicator")
dbutils.widgets.text("processName", "", "Process Name")
dbutils.widgets.text("jobId", "", "Job Id")
dbutils.widgets.text("jobRunId", "", "Job Run Id")
dbutils.widgets.text("processStartTime", "", "Process Start Time")
dbutils.widgets.text("processEndTime", "", "Process End Time")
dbutils.widgets.text("taskRunId", "", "Task Run Id")
dbutils.widgets.dropdown("processStatus", "Started", ["Started", "Completed", "Skipped"], "Process Status")

# COMMAND ----------

if dbutils.widgets.get("skip_ind") == "True":
  dbutils.notebook.exit('Process skipped')

# COMMAND ----------

# Retrieve the widget value in Python
processName = dbutils.widgets.get("processName")
processStatus = dbutils.widgets.get("processStatus")
jobRunId = dbutils.widgets.get("jobRunId")
taskRunId = dbutils.widgets.get("taskRunId")
jobId = dbutils.widgets.get("jobId")
now_datetime = datetime.now()

if processStatus == "Completed" or processStatus == 'Skipped':
  sql_commmand = f"""
  update rocket_mortgage_catalog.common.pipeline_watermarklog
  set ProcessStatus = '{processStatus}'
  , RecordUpdateDateTime = '{now_datetime}'
  where JobRunId = '{jobRunId}'
  """
  spark.sql(sql_commmand)
  dbutils.notebook.exit('Process completed')

# Use the retrieved value in the SQL query
query = f"""
INSERT INTO rocket_mortgage_catalog.common.pipeline_watermarklog (ProcessName, ProcessStatus, JobRunId, TaskRunId, JobId)
SELECT '{processName}', '{processStatus}', '{jobRunId}', '{taskRunId}', '{jobId}'
"""

# Execute the SQL query
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rocket_mortgage_catalog.common.pipeline_watermarklog
# MAGIC order by RecordInsertDateTime desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC   update rocket_mortgage_catalog.common.pipeline_watermarklog
# MAGIC   set ProcessStatus = 'COMPLETED'
# MAGIC   , RecordUpdateDateTime = getdate()
# MAGIC   where JobRunId = 'tesasdfasd'
