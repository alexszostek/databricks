-- Databricks notebook source
use gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table LeadDim DDL 

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.LeadDim(
	LeadSK bigint GENERATED ALWAYS   AS IDENTITY (START WITH 1000 INCREMENT BY 1) ,
	LeadID string ,
	LeadName string ,
    LeadSource string,
	LeadCityName string ,
	LeadStateCode string ,
	LeadStateName string ,
	LeadZipCode string ,
	LeadCountryCode string ,
	--LeadDesignatedMarketArea string ,
	--LeadInterestReasonDesc string ,
	LeadHasOptedOutofEmailFlag string ,
	LeadConvertedDate Timestamp ,
    LeadIsConvertedFlag string ,
	LeadIsDoNotCallFlag string ,
	--LeadIsEmailOnlyFlag string ,
	--LeadIsHouston50MileRadiusFlag string ,
	--LeadIsHouston100MileRadiusFlag string ,
	--LeadIsHouston200MileRadiusFlag string ,
	--LeadHasTCPAConsentFlag string ,
	--LeadIsActiveTestMarketFlag string ,
	--LeadIsCallableFlag string ,
	IsCurrentRecordInd boolean  ,
	IsSourceRecordDeletedInd boolean ,
	RecordEffectiveDateTime timestamp  ,
	RecordExpirationDateTime timestamp  ,
	ETLInsertBatchID string NOT NULL GENERATED ALWAYS AS ('-1') ,
	RecordInsertDateTime timestamp  ,
	RecordInsertUserName string  GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) ,
	ETLUpdateBatchID string NOT NULL GENERATED ALWAYS AS ('-1') ,
	RecordUpdateDateTime timestamp  ,
	RecordUpdateUserName string   GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)),
	RecordCheckSumNumber string 
	)
