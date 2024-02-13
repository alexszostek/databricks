-- Databricks notebook source
use gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table TimeDim DDL 

-- COMMAND ----------

CREATE OR REPLACE  TABLE gold.RecordTypeDim (
RecordTypeSK bigint GENERATED ALWAYS   AS IDENTITY (START WITH 1000 INCREMENT BY 1)
,RecordTypeId string
,RecordTypeName string
,RecordTypeDescription string
,RecordTypeSubjectType string
,IsCurrentRecordInd boolean
,RecordEffectiveDateTime timestamp 
,RecordExpirationDateTime timestamp 
,ETLInsertBatchID string NOT NULL GENERATED ALWAYS AS ('-1') 
,RecordInsertDateTime timestamp 
,RecordInsertUserName string  GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) 
,ETLUpdateBatchID string NOT NULL GENERATED ALWAYS AS ('-1') 
,RecordUpdateDateTime timestamp 
,RecordUpdateUserName string   GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))
,RecordCheckSumNumber string NOT NULL
);
