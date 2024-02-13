-- Databricks notebook source
use silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforceuser DDL 

-- COMMAND ----------

CREATE OR REPlACE TABLE silver.RocketAutoSalesforcePayment (
Id string ,
IsDeleted boolean ,
Name string ,
CreatedDate timestamp ,
CreatedById string ,
LastModifiedDate timestamp ,
LastModifiedById string ,
SystemModstamp timestamp ,
Opportunity__c string ,
URL__c string ,
Payment_Status__c string ,
Payment_Amount__c decimal(38,18) ,
Payment_Reason__c string ,
Created_Date__c timestamp ,
Account__c string ,
PaymentEventType__c string ,
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


--Alter table Silver.RocketAutoSalesforcePayment
--Add columns 
--(test string) 
