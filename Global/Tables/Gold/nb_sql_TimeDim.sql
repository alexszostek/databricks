-- Databricks notebook source
use gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table TimeDim DDL 

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.TimeDim(
	TimeSK int ,
	Time string ,
	SerialTime int ,
	HourSK int ,
	MinuteName string ,
	MinuteNumber smallint ,
	SecondName string ,
	SecondNumber smallint ,
	AMPMTimeCode string ,
	Hour24Name string ,
	Hour24Number smallint ,
	Hour24HHMMTimeName string ,
	Hour24HHMMFormattedTimeName string ,
	Hour24HHMMSSTimeName string ,
	Hour24HHMMSSFormattedTimeName string ,
	Hour12Name string ,
	Hour12Number smallint ,
	Hour12HHMMTimeName string ,
	Hour12HHMMFormattedTimeName string ,
	Hour12HHMMSSTimeName string ,
	Hour12HHMMSSFormattedTimeName string ,
	TimeDesc string ,
	IsCurrentRecordInd boolean ,
	IsSourceRecordDeletedInd boolean ,
	RecordEffectiveDateTime timestamp ,
	RecordExpirationDateTime timestamp ,
	ETLInsertBatchID bigint ,
	RecordInsertDateTime timestamp ,
	RecordInsertUserName string ,
	ETLUpdateBatchID bigint ,
	RecordUpdateDateTime timestamp ,
	RecordUpdateUserName string ,
	RecordCheckSumNumber string 
	)

-- COMMAND ----------

Optimize gold.DateDim zorder by (DateSK,DayDate);

