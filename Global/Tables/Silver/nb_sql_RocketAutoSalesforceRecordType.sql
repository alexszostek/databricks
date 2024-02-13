-- Databricks notebook source
use silver

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from datetime import *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforceRecordType DDL 

-- COMMAND ----------

CREATE OR REPlACE TABLE silver.RocketAutoSalesforceRecordType (
Id        string ,
BusinessProcessId        string ,
CreatedById        string ,
CreatedDate        timestamp ,
Description        string ,
DeveloperName        string ,
IsActive        boolean ,
IsPersonType        boolean ,
LastModifiedById        string ,
LastModifiedDate        timestamp ,
Name        string ,
NamespacePrefix        string ,
SobjectType        string ,
SystemModstamp        timestamp ,
RecordChecksumNumber string ,
ETLInsertBatchID string GENERATED ALWAYS AS ('-1') ,
RecordInsertDateTime timestamp  , 
RecordInsertUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) ,
ETLUpdateBatchID string GENERATED ALWAYS AS ('-1') ,
RecordUpdateDateTime timestamp  ,
RecordUpdateUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))  
 );    

-- COMMAND ----------

ALTER TABLE silver.RocketAutoSalesforceRecordType 
SET TBLPROPERTIES('delta.minReaderVersion' = '1', 'delta.minWriterVersion'='7');
ALTER TABLE silver.RocketAutoSalesforceRecordType  SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'enabled');
ALTER TABLE silver.RocketAutoSalesforceRecordType  ALTER COLUMN RecordInsertDateTime SET  DEFAULT Cast(CURRENT_TIMESTAMP() As timestamp);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Add Columns DDL

-- COMMAND ----------


--Alter table Silver.RocketAutoSalesforceRecordType
--Add columns 
--(test string) 
