-- Databricks notebook source
use gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table LeadFact DDL 

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.LeadFact(
 LeadSK int 
,LeadID string
,RecordTypeId string
,RecordTypeSK int
,CreateESTDate Date
,CreateESTTime string 
,CreateESTDateSK int 
,CreateESTTimeSK int 
,ConvertedESTDate Date
,ConvertetESTTime string
,ConvertedESTDateSK int 
,ConvertedESTTimeSK int 
,LeadQuantity int NOT NULL
,LeadAgeInSecondsQuantity bigint 
--,LeadIsEmailOnlyQuantity int NOT NULL
,LeadIsConvertedQuantity int NOT NULL
,LeadIsDoNotCallQuantity int NOT NULL
,ETLInsertBatchID  string NOT NULL GENERATED ALWAYS AS ('-1')
,RecordInsertDateTime timestamp NOT NULL
,RecordInsertUserName string  GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))
,ETLUpdateBatchID string NOT NULL GENERATED ALWAYS AS ('-1')
,RecordUpdateDateTime timestamp NOT NULL
,RecordUpdateUserName string  GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))
,RecordCheckSumNumber string NOT NULL
)
