-- Databricks notebook source
use silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforcetask DDL 

-- COMMAND ----------

CREATE OR REPlACE TABLE silver.RocketAutoSalesforceTask (
Id        string ,
AccountId        string ,
ActivityDate        timestamp ,
CallDisposition        string ,
CallDurationInSeconds        int ,
CallObject        string ,
CallType        string ,
CompletedDateTime        timestamp ,
CreatedById        string ,
CreatedDate        timestamp ,
Description        string ,
Five9_List__c        string ,
Five9__Five9AgentExtension__c        string ,
Five9__Five9AgentName__c        string ,
Five9__Five9Agent__c        string ,
Five9__Five9ANI__c        string ,
Five9__Five9CallType__c        string ,
Five9__Five9Campaign__c        string ,
Five9__Five9DNIS__c        string ,
Five9__Five9HandleTime__c        string ,
Five9__Five9SessionId__c        string ,
Five9__Five9TalkAndHoldTimeInSeconds__c        decimal(38,18) ,
Five9__Five9TaskType__c        string ,
Five9__Five9WrapTime__c        string ,
icrt__AeGuide_Name__c        string ,
IsArchived        boolean ,
IsClosed        boolean ,
IsDeleted        boolean ,
IsHighPriority        boolean ,
IsRecurrence        boolean ,
IsReminderSet        boolean ,
LastModifiedById        string ,
LastModifiedDate        timestamp ,
Opportunity__c        string ,
OwnerId        string ,
Priority        string ,
RecurrenceActivityId        string ,
RecurrenceDayOfMonth        int ,
RecurrenceDayOfWeekMask        int ,
RecurrenceEndDateOnly        timestamp ,
RecurrenceInstance        string ,
RecurrenceInterval        int ,
RecurrenceMonthOfYear        string ,
RecurrenceRegeneratedType        string ,
RecurrenceStartDateOnly        timestamp ,
RecurrenceTimeZoneSidKey        string ,
RecurrenceType        string ,
ReminderDateTime        timestamp ,
Status        string ,
Subject        string ,
SystemModstamp        timestamp ,
TaskSubtype        string ,
WhatCount        int ,
WhatId        string ,
WhoCount        int ,
WhoId        string ,
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


--Alter table Silver.RocketAutoSalesforceTask
--Add columns 
--(test string) 
