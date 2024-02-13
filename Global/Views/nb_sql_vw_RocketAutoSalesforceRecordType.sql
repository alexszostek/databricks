-- Databricks notebook source
use silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforceRecordType DDL 

-- COMMAND ----------

CREATE OR REPlACE TABLE silver.RocketAutoSalesforceRecordType (
        BusinessProcessId string ,
        CreatedById string ,
        CreatedDate timestamp ,
        Description string ,
        DeveloperName string ,
        Id string  ,
        IsActive boolean ,
        LastModifiedById string ,
        LastModifiedDate timestamp ,
        Name string ,
        NamespacePrefix string ,
        SobjectType string ,
        SystemModstamp timestamp ,
        RecordChecksumNumber string ,
        ETLInsertBatchID string GENERATED ALWAYS AS ('-1') ,
        RecordInsertDateTime string ,
        RecordInsertUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) ,
        ETLUpdateBatchID string GENERATED ALWAYS AS ('-1') ,
        RecordUpdateDateTime string  ,
        RecordUpdateUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))  
        )
     

-- COMMAND ----------

Optimize silver.RocketAutoSalesforceRecordType
