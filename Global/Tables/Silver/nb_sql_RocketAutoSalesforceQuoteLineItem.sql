-- Databricks notebook source
use silver;

-- describe table bronze.rasf_quotelineitem

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforceQuoteLineItem DDL 

-- COMMAND ----------

CREATE OR REPlACE TABLE silver.RocketAutoSalesforceQuoteLineItem (
Id string,
IsDeleted boolean,
LineNumber string,
CreatedDate timestamp,
CreatedById string,
LastModifiedDate timestamp,
LastModifiedById string,
SystemModstamp timestamp,
LastViewedDate timestamp,
LastReferencedDate timestamp,
QuoteId string,
PricebookEntryId string,
OpportunityLineItemId string,
Quantity decimal(38,18),
UnitPrice decimal(38,18),
Discount decimal(38,18),
Description string,
ServiceDate timestamp,
Product2Id string,
SortOrder int,
ListPrice decimal(38,18),
Subtotal decimal(38,18),
TotalPrice decimal(38,18),
RecordChecksumNumber string ,
ETLInsertBatchID string GENERATED ALWAYS AS ('-1') ,
RecordInsertDateTime timestamp ,
RecordInsertUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) ,
ETLUpdateBatchID string GENERATED ALWAYS AS ('-1') ,
RecordUpdateDateTime timestamp  ,
RecordUpdateUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))  
);
     

-- COMMAND ----------


