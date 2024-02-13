-- Databricks notebook source
use silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforceContact DDL 

-- COMMAND ----------

CREATE OR REPlACE TABLE silver.RocketAutoSalesforceContact (
Id        string ,
IsDeleted        boolean ,
MasterRecordId        string ,
AccountId        string ,
IsPersonAccount        boolean ,
LastName        string ,
FirstName        string ,
Salutation        string ,
MiddleName        string ,
Suffix        string ,
Name        string ,
RecordTypeId        string ,
MailingStreet        string ,
MailingCity        string ,
MailingState        string ,
MailingPostalCode        string ,
MailingCountry        string ,
MailingStateCode        string ,
MailingCountryCode        string ,
MailingLatitude        decimal(38,18) ,
MailingLongitude        decimal(38,18) ,
MailingGeocodeAccuracy        string ,
Phone        string ,
Fax        string ,
MobilePhone        string ,
ReportsToId        string ,
Email        string ,
Title        string ,
Department        string ,
OwnerId        string ,
HasOptedOutOfEmail        boolean ,
CreatedDate        timestamp ,
CreatedById        string ,
LastModifiedDate        timestamp ,
LastModifiedById        string ,
SystemModstamp        timestamp ,
LastActivityDate        timestamp ,
LastCURequestDate        timestamp ,
LastCUUpdateDate        timestamp ,
LastViewedDate        timestamp ,
LastReferencedDate        timestamp ,
EmailBouncedReason        string ,
EmailBouncedDate        timestamp ,
IsEmailBounced        boolean ,
PhotoUrl        string ,
Jigsaw        string ,
JigsawContactId        string ,
et4ae5__HasOptedOutOfMobile__c        boolean ,
et4ae5__Mobile_Country_Code__c        string ,
Title__c        string ,
Dealer__c        string ,
Active__c        boolean ,
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


--Alter table Silver.RocketAutoSalesforceContact
--Add columns 
--(test string) 
