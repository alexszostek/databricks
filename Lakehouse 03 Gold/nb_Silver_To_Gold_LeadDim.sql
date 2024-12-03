-- Databricks notebook source
-- MAGIC %md
-- MAGIC                                                                     Refresh Source Table

-- COMMAND ----------


Refresh Table Silver.rocketautosalesforcelead

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Build Integrate View

-- COMMAND ----------

CREATE Or Replace View integrate_rocketautosalesforcelead as 
Select 
Id as Mergekey
,Id as LeadID
,Name  as LeadName
,LeadSource as LeadSource
,City as LeadCityName
,State as LeadStateCode
,PostalCode as LeadZipCode
,Country as LeadCountryCode
,HasOptedOutOfEmail as LeadHasOptedOutofEmailFlag
,IsConverted as LeadIsConvertedFlag
,DoNotCall as LeadIsDoNotCallFlag
,IsDeleted as IsSourceRecordDeletedInd
,ConvertedDate as LeadConvertedDate
,md5(concat_ws("",Id,Name,LeadSource,City,State,PostalCode,Country,HasOptedOutOfEmail,IsConverted,DoNotCall,IsDeleted,ConvertedDate)) as RecordCheckSumNumber
from Silver.rocketautosalesforcelead
union 
select
'null' as Mergekey
,Id as LeadID
,Name  as LeadName
,s.LeadSource as LeadSource
,City as LeadCityName
,State as LeadStateCode
,PostalCode as LeadZipCode
,Country as LeadCountryCode
,HasOptedOutOfEmail as LeadHasOptedOutofEmailFlag
,IsConverted as LeadIsConvertedFlag
,DoNotCall as LeadIsDoNotCallFlag
,IsDeleted as IsSourceRecordDeletedInd
,ConvertedDate as LeadConvertedDate
,md5(concat_ws("",Id,Name,s.LeadSource,City,State,PostalCode,Country,HasOptedOutOfEmail,IsConverted,DoNotCall,IsDeleted,ConvertedDate)) as RecordCheckSumNumber
from Silver.rocketautosalesforcelead as s
join gold.leaddim as g on g.LeadID = s.Id
where g.RecordExpirationDateTime = '9999-01-01T00:00:00.000+0000' and g.RecordCheckSumNumber <> md5(concat_ws("",s.Id,s.Name,s.LeadSource,s.City,s.State,s.PostalCode,s.Country,s.HasOptedOutOfEmail,s.IsConverted,s.DoNotCall,s.IsDeleted,s.ConvertedDate))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Merge Script

-- COMMAND ----------

merge into gold.leaddim as trg
using integrate_rocketautosalesforcelead as src
on trg.LeadID = src.Mergekey

--------///UPDATE//--------
when matched and trg.RecordCheckSumNumber <> src.RecordCheckSumNumber
then update set

 trg.RecordExpirationDateTime         =  current_timestamp()
,trg.RecordUpdateDateTime             =  current_timestamp() --date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss' )--src.RecordUpdateDateTime 
,trg.IsCurrentRecordInd               = 'false'

--------///INSERT//--------
when not matched then insert 
(
trg.LeadID
,trg.LeadName
,trg.LeadSource
,trg.LeadCityName
,trg.LeadStateCode
,trg.LeadZipCode
,trg.LeadCountryCode
,trg.LeadHasOptedOutofEmailFlag
,trg.LeadConvertedDate
,trg.LeadIsConvertedFlag
,trg.LeadIsDoNotCallFlag
,trg.IsCurrentRecordInd
,trg.IsSourceRecordDeletedInd
,trg.RecordEffectiveDateTime
,trg.RecordExpirationDateTime
,trg.RecordInsertDateTime 
,trg.RecordUpdateDateTime
,trg.RecordCheckSumNumber)
values(

src.LeadID
,src.LeadName
,src.LeadSource
,src.LeadCityName
,src.LeadStateCode
,src.LeadZipCode
,src.LeadCountryCode
,src.LeadHasOptedOutofEmailFlag
,src.LeadConvertedDate
,src.LeadIsConvertedFlag
,src.LeadIsDoNotCallFlag
,'true' ---src,IsCurrentRecordInd
,src.IsSourceRecordDeletedInd
,current_timestamp() ---src.RecordEffectiveDateTime
,'9999-01-01T00:00:00.000+0000'--src.RecordExpirationDateTime
,current_timestamp()--date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss')--src.RecordInsertDateTime
,current_timestamp()--date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss')--src.RecordUpdateDateTime 
,src.RecordCheckSumNumber

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                     Drop Integrate View

-- COMMAND ----------


drop view  if exists integrate_rocketautosalesforcelead

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Data Validation

-- COMMAND ----------

select Count(*) from gold.leaddim

--truncate table gold.leaddim

-- COMMAND ----------

-- MAGIC %md
-- MAGIC                                                                      Table Optimize

-- COMMAND ----------

--optimize gold.leaddim zorder by (LeadID,LeadSK)
