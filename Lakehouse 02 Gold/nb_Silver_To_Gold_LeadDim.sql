-- Databricks notebook source
-- MAGIC %md
-- MAGIC                                                                     Refresh Source Table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC Refresh Table Silver.rocketautosalesforcelead

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Build Integrate View

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE Or Replace View integrate_rocketautosalesforcelead as 
-- MAGIC Select 
-- MAGIC Id as Mergekey
-- MAGIC ,Id as LeadID
-- MAGIC ,Name  as LeadName
-- MAGIC ,LeadSource as LeadSource
-- MAGIC ,City as LeadCityName
-- MAGIC ,State as LeadStateCode
-- MAGIC ,PostalCode as LeadZipCode
-- MAGIC ,Country as LeadCountryCode
-- MAGIC ,HasOptedOutOfEmail as LeadHasOptedOutofEmailFlag
-- MAGIC ,IsConverted as LeadIsConvertedFlag
-- MAGIC ,DoNotCall as LeadIsDoNotCallFlag
-- MAGIC ,IsDeleted as IsSourceRecordDeletedInd
-- MAGIC ,ConvertedDate as LeadConvertedDate
-- MAGIC ,md5(concat_ws("",Id,Name,LeadSource,City,State,PostalCode,Country,HasOptedOutOfEmail,IsConverted,DoNotCall,IsDeleted,ConvertedDate)) as RecordCheckSumNumber
-- MAGIC from Silver.rocketautosalesforcelead
-- MAGIC union 
-- MAGIC select
-- MAGIC 'null' as Mergekey
-- MAGIC ,Id as LeadID
-- MAGIC ,Name  as LeadName
-- MAGIC ,s.LeadSource as LeadSource
-- MAGIC ,City as LeadCityName
-- MAGIC ,State as LeadStateCode
-- MAGIC ,PostalCode as LeadZipCode
-- MAGIC ,Country as LeadCountryCode
-- MAGIC ,HasOptedOutOfEmail as LeadHasOptedOutofEmailFlag
-- MAGIC ,IsConverted as LeadIsConvertedFlag
-- MAGIC ,DoNotCall as LeadIsDoNotCallFlag
-- MAGIC ,IsDeleted as IsSourceRecordDeletedInd
-- MAGIC ,ConvertedDate as LeadConvertedDate
-- MAGIC ,md5(concat_ws("",Id,Name,s.LeadSource,City,State,PostalCode,Country,HasOptedOutOfEmail,IsConverted,DoNotCall,IsDeleted,ConvertedDate)) as RecordCheckSumNumber
-- MAGIC from Silver.rocketautosalesforcelead as s
-- MAGIC join gold.leaddim as g on g.LeadID = s.Id
-- MAGIC where g.RecordExpirationDateTime = '9999-01-01T00:00:00.000+0000' and g.RecordCheckSumNumber <> md5(concat_ws("",s.Id,s.Name,s.LeadSource,s.City,s.State,s.PostalCode,s.Country,s.HasOptedOutOfEmail,s.IsConverted,s.DoNotCall,s.IsDeleted,s.ConvertedDate))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Merge Script

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC merge into gold.leaddim as trg
-- MAGIC using integrate_rocketautosalesforcelead as src
-- MAGIC on trg.LeadID = src.Mergekey
-- MAGIC 
-- MAGIC --------///UPDATE//--------
-- MAGIC when matched and trg.RecordCheckSumNumber <> src.RecordCheckSumNumber
-- MAGIC then update set
-- MAGIC 
-- MAGIC  trg.RecordExpirationDateTime         =  current_timestamp()
-- MAGIC ,trg.RecordUpdateDateTime             =  current_timestamp() --date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss' )--src.RecordUpdateDateTime 
-- MAGIC ,trg.IsCurrentRecordInd               = 'false'
-- MAGIC 
-- MAGIC --------///INSERT//--------
-- MAGIC when not matched then insert 
-- MAGIC (
-- MAGIC trg.LeadID
-- MAGIC ,trg.LeadName
-- MAGIC ,trg.LeadSource
-- MAGIC ,trg.LeadCityName
-- MAGIC ,trg.LeadStateCode
-- MAGIC ,trg.LeadZipCode
-- MAGIC ,trg.LeadCountryCode
-- MAGIC ,trg.LeadHasOptedOutofEmailFlag
-- MAGIC ,trg.LeadConvertedDate
-- MAGIC ,trg.LeadIsConvertedFlag
-- MAGIC ,trg.LeadIsDoNotCallFlag
-- MAGIC ,trg.IsCurrentRecordInd
-- MAGIC ,trg.IsSourceRecordDeletedInd
-- MAGIC ,trg.RecordEffectiveDateTime
-- MAGIC ,trg.RecordExpirationDateTime
-- MAGIC ,trg.RecordInsertDateTime 
-- MAGIC ,trg.RecordUpdateDateTime
-- MAGIC ,trg.RecordCheckSumNumber)
-- MAGIC values(
-- MAGIC 
-- MAGIC src.LeadID
-- MAGIC ,src.LeadName
-- MAGIC ,src.LeadSource
-- MAGIC ,src.LeadCityName
-- MAGIC ,src.LeadStateCode
-- MAGIC ,src.LeadZipCode
-- MAGIC ,src.LeadCountryCode
-- MAGIC ,src.LeadHasOptedOutofEmailFlag
-- MAGIC ,src.LeadConvertedDate
-- MAGIC ,src.LeadIsConvertedFlag
-- MAGIC ,src.LeadIsDoNotCallFlag
-- MAGIC ,'true' ---src,IsCurrentRecordInd
-- MAGIC ,src.IsSourceRecordDeletedInd
-- MAGIC ,current_timestamp() ---src.RecordEffectiveDateTime
-- MAGIC ,'9999-01-01T00:00:00.000+0000'--src.RecordExpirationDateTime
-- MAGIC ,current_timestamp()--date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss')--src.RecordInsertDateTime
-- MAGIC ,current_timestamp()--date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss')--src.RecordUpdateDateTime 
-- MAGIC ,src.RecordCheckSumNumber
-- MAGIC 
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                     Drop Integrate View

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC drop view  if exists integrate_rocketautosalesforcelead

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Data Validation

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select Count(*) from gold.leaddim
-- MAGIC 
-- MAGIC --truncate table gold.leaddim

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Table Optimize

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --optimize gold.leaddim zorder by (LeadID,LeadSK)
