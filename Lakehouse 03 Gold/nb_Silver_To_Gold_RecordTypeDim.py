# Databricks notebook source
# MAGIC %md
# MAGIC                                                                     Refresh Source Table

# COMMAND ----------

# MAGIC %sql
# MAGIC Refresh table silver.rocketautosalesforcerecordtype

# COMMAND ----------

# MAGIC %md
# MAGIC                                                                            Data Frame

# COMMAND ----------

dfsrc =spark.read.table("silver.rocketautosalesforcerecordtype")
dftrg =spark.read.table("gold.recordtypedim")
display(dfsrc)
##display(dftrg)


# COMMAND ----------

# MAGIC %md
# MAGIC                                                                     Integrate View Script

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS integrate_rocketautosalesforcerecordtype as 
# MAGIC Select 
# MAGIC  Id as Mergekey
# MAGIC ,Id as RecordTypeId
# MAGIC ,Name as RecordTypeName
# MAGIC ,Description as RecordTypeDescription
# MAGIC ,IsActive as IsCurrentRecordInd
# MAGIC ,SobjectType AS RecordTypeSubjectType
# MAGIC ,md5(concat_ws("",Id,Name,Description,IsActive,SobjectType)) as RecordCheckSumNumber
# MAGIC from silver.rocketautosalesforcerecordtype
# MAGIC --where 
# MAGIC union
# MAGIC select 
# MAGIC 'null' as Mergekey
# MAGIC ,Id as RecordTypeId
# MAGIC ,Name as RecordTypeName
# MAGIC ,Description as RecordTypeDescription
# MAGIC ,IsActive as IsCurrentRecordInd
# MAGIC ,SobjectType AS RecordTypeSubjectType
# MAGIC ,md5(concat_ws("",Id,Name,Description,IsActive,SobjectType)) as RecordCheckSumNumber
# MAGIC from silver.rocketautosalesforcerecordtype as s
# MAGIC join gold.recordtypedim as g on  g.RecordTypeId = s.id
# MAGIC where g.RecordExpirationDateTime = '9999-01-01T00:00:00.000+0000'  and g.RecordCheckSumNumber <> md5(concat_ws("",s.Id,s.Name,s.Description,s.IsActive,s.SobjectType))

# COMMAND ----------

# MAGIC %md
# MAGIC                                                                     Merge Script

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into gold.recordtypedim as trg
# MAGIC using integrate_rocketautosalesforcerecordtype as src
# MAGIC on trg.RecordTypeId = src.Mergekey
# MAGIC
# MAGIC --------///UPDATE//--------
# MAGIC when matched and trg.RecordCheckSumNumber <> src.RecordCheckSumNumber
# MAGIC then update set
# MAGIC
# MAGIC --trg.RecordTypeSK                    = src.RecordTypeSK         
# MAGIC  trg.RecordExpirationDateTime         =  current_timestamp()
# MAGIC ,trg.RecordUpdateDateTime             =  current_timestamp() --date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss' )--src.RecordUpdateDateTime 
# MAGIC ,trg.IsCurrentRecordInd               = 'false'
# MAGIC
# MAGIC --------///INSERT//--------
# MAGIC when not matched then insert 
# MAGIC (
# MAGIC  trg.RecordTypeId         
# MAGIC ,trg.RecordTypeName       
# MAGIC ,trg.RecordTypeDescription
# MAGIC ,trg.RecordTypeSubjectType
# MAGIC ,trg.IsCurrentRecordInd  
# MAGIC ,trg.RecordInsertDateTime 
# MAGIC ,trg.RecordUpdateDateTime
# MAGIC ,trg.RecordEffectiveDateTime
# MAGIC ,trg.RecordExpirationDateTime
# MAGIC ,trg.RecordCheckSumNumber )
# MAGIC VALUES (
# MAGIC  src.RecordTypeId         
# MAGIC ,src.RecordTypeName       
# MAGIC ,src.RecordTypeDescription
# MAGIC ,src.RecordTypeSubjectType
# MAGIC ,src.IsCurrentRecordInd  
# MAGIC ,current_timestamp()--date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss')--src.RecordInsertDateTime
# MAGIC ,current_timestamp()--date_format(current_timestamp(),'yyyy-mm-dd HH:mm:ss')--src.RecordUpdateDateTime 
# MAGIC ,current_timestamp() ---src.RecordEffectiveDateTime
# MAGIC ,'9999-01-01T00:00:00.000+0000' --trg.RecordExpirationDateTime   
# MAGIC ,src.RecordCheckSumNumber )

# COMMAND ----------

# MAGIC %md
# MAGIC                                                                     Drop Integrate View

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view  if exists  integrate_rocketautosalesforcerecordtype

# COMMAND ----------

# MAGIC %md
# MAGIC                                                                    Data Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC --select RecordEffectiveDateTime,* from gold.recordtypedim
# MAGIC --order by recordtypesk

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from silver.rocketautosalesforcerecordtype

# COMMAND ----------

# MAGIC %md
# MAGIC                                                                     Table Optimize

# COMMAND ----------

# MAGIC %sql --1.18  --> 0.46
# MAGIC --optimize gold.recordtypedim zorder by (RecordTypeId,RecordTypeSK)

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_add(current_date(), -1)
