-- Databricks notebook source
use silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforceOpportunityFieldHistory DDL 

-- COMMAND ----------

CREATE OR REPlACE TABLE silver.RocketAutoSalesforceOpportunityFieldHistory (
Id        string ,
IsDeleted        boolean ,
OpportunityId        string ,
CreatedById        string ,
CreatedDate        timestamp ,
Field        string ,
DataType        string ,
OldValue        string ,
NewValue        string ,
RecordChecksumNumber string ,
ETLInsertBatchID string GENERATED ALWAYS AS ('-1') ,
RecordInsertDateTime timestamp ,
RecordInsertUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) ,
ETLUpdateBatchID string GENERATED ALWAYS AS ('-1') ,
RecordUpdateDateTime timestamp  ,
RecordUpdateUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))  
 );
     

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Add Columns DDL

-- COMMAND ----------


--Alter table Silver.RocketAutoSalesforceOpportunityFieldHistory
--Add columns 
--(test string) 
