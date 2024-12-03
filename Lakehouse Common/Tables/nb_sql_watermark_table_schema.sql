-- Databricks notebook source
Create
or replace table rocket_mortgage_catalog.common.Pipeline_WatermarkLog (
  processLogId bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000 INCREMENT BY 1),
  processName string,
  jobId string,
  jobRunId string,
  taskRunId string,
  processStartTime timestamp,
  processEndTime timestamp,
  processStatus string,
  recordInsertDateTime timestamp,
  recordUpdateDateTime timestamp
)

-- COMMAND ----------

ALTER TABLE
  rocket_mortgage_catalog.common.Pipeline_WatermarkLog
SET
  TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');
ALTER TABLE
  rocket_mortgage_catalog.common.Pipeline_WatermarkLog
ALTER COLUMN
  RecordInsertDateTime
SET
  DEFAULT current_timestamp();
ALTER TABLE
  rocket_mortgage_catalog.common.Pipeline_WatermarkLog
ALTER COLUMN
  RecordUpdateDateTime
SET
  DEFAULT current_timestamp();

-- COMMAND ----------

select
  *,
  unix_timestamp(RecordInsertDateTime) - unix_timestamp(RecordUpdateDateTime) as difference_in_seconds
from
  common.pipeline_watermarklog
order by
  RecordInsertDateTime desc
