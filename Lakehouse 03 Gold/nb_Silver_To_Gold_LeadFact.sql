-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Refresh Source Table

-- COMMAND ----------

Refresh silver.rocketautosalesforcelead

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Build Integrate View

-- COMMAND ----------

CREATE Or Replace VIEW integrate_rasflead as  
select 
 A.*
,md5(concat_ws("",A.*)) as RecordCheckSumNumber

from  (
select 
 srl.Id as LeadID
,ld.LeadSK
,srl.RecordTypeId
,gr.RecordTypeSK
,to_date(from_utc_timestamp(srl.CreatedDate,'America/New_York')) as CreateESTDate
,date_format(from_utc_timestamp(srl.CreatedDate,'America/New_York'),'HH:mm:ss') as CreateESTTime
,gd.DateSK as CreateESTDateSK
,gt.TimeSK as CreateESTTimeSK
,srl.ConvertedDate
,to_date(from_utc_timestamp(srl.ConvertedDate,'America/New_York'))  as ConvertedESTDate
,date_format(from_utc_timestamp(srl.ConvertedDate,'America/New_York'),'HH:mm:ss') as ConvertetESTTime
,gdc.DateSK as ConvertedESTDateSK
,gtc.TimeSK as ConvertedESTTimeSK
, 1 as LeadQuantity
,ifnull(case when srl.IsConverted is true then datediff(second,from_utc_timestamp(srl.ConvertedDate,'America/New_York'),from_utc_timestamp(srl.CreatedDate,'America/New_York'))
        else datediff(second,from_utc_timestamp(srl.ConvertedDate,'America/New_York'),from_utc_timestamp(current_timestamp(),'America/New_York'))
        end ,0) as LeadAgeInSecondsQuantity
,case when srl.IsConverted is true then 1 else 0 end as LeadIsConvertedQuantity
,case when srl.DoNotCall is true then 1 else 0 end as LeadIsDoNotCallQuantity
from silver.rocketautosalesforcelead as srl
left join gold.datedim as gd on to_date(from_utc_timestamp(srl.CreatedDate,'America/New_York')) = gd.DayDate 
left join gold.datedim as gdc on to_date(from_utc_timestamp(srl.ConvertedDate,'America/New_York')) = gdc.DayDate 
left join gold.timedim as gt on date_format(from_utc_timestamp(srl.CreatedDate,'America/New_York'),'HH:mm:ss') = gt.Time
left join gold.timedim as gtc on date_format(from_utc_timestamp(srl.ConvertedDate,'America/New_York'),'HH:mm:ss') = gtc.Time
left join gold.recordtypedim gr on srl.RecordTypeId = gr.RecordTypeId
left join gold.leaddim as ld on srl.id = ld.LeadID and ld.IsCurrentRecordInd is true and ld.RecordExpirationDateTime = '9999-01-01T00:00:00.000+0000'
--where srl.RecordUpdateDateTime
) A;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Merge Script

-- COMMAND ----------

merge into gold.LeadFact as trg
using integrate_rasflead as src
on trg.LeadID = src.LeadID

--------///UPDATE//--------
when matched and trg.RecordCheckSumNumber <> src.RecordCheckSumNumber
then update set 
 trg.LeadSK                                   =src.LeadSk
,trg.LeadID                                   =src.LeadID
,trg.RecordTypeId                             =src.RecordTypeId
,trg.RecordTypeSK                             =src.RecordTypeSK
,trg.CreateESTDate                            =src.CreateESTDate
,trg.CreateESTTime                            =src.CreateESTTime
,trg.CreateESTDateSK                          =src.CreateESTDateSK
,trg.CreateESTTimeSK                          =src.CreateESTTimeSK
,trg.ConvertedESTDate                         =src.ConvertedESTDate
,trg.ConvertetESTTime                         =src.ConvertetESTTime
,trg.ConvertedESTDateSK                       =src.ConvertedESTDateSK
,trg.ConvertedESTTimeSK                       =src.ConvertedESTTimeSK
,trg.LeadQuantity                             =src.LeadQuantity
,trg.LeadAgeInSecondsQuantity                 =src.LeadAgeInSecondsQuantity
,trg.LeadIsConvertedQuantity                  =src.LeadIsConvertedQuantity
,trg.LeadIsDoNotCallQuantity                  =src.LeadIsDoNotCallQuantity
--,trg.RecordInsertDateTime                   =src.RecordInsertDateTime
,trg.RecordUpdateDateTime                     = current_timestamp()--src.RecordUpdateDateTime
,trg.RecordCheckSumNumber                     =src.RecordCheckSumNumber


--------///INSERT//--------
when not matched then INSERT ( 
 trg.LeadSK                                   
,trg.LeadID                                  
,trg.RecordTypeId                            
,trg.RecordTypeSK                            
,trg.CreateESTDate                           
,trg.CreateESTTime                           
,trg.CreateESTDateSK                        
,trg.CreateESTTimeSK                        
,trg.ConvertedESTDate                        
,trg.ConvertetESTTime                        
,trg.ConvertedESTDateSK                      
,trg.ConvertedESTTimeSK                      
,trg.LeadQuantity                            
,trg.LeadAgeInSecondsQuantity                
,trg.LeadIsConvertedQuantity                 
,trg.LeadIsDoNotCallQuantity                 
,trg.RecordInsertDateTime                  
,trg.RecordUpdateDateTime                    
,trg.RecordCheckSumNumber    
) VALUES ( 
src.LeadSK                                   
,src.LeadID                                  
,src.RecordTypeId                            
,src.RecordTypeSK                            
,src.CreateESTDate                           
,src.CreateESTTime                           
,src.CreateESTDateSK                        
,src.CreateESTTimeSK                        
,src.ConvertedESTDate                        
,src.ConvertetESTTime                        
,src.ConvertedESTDateSK                      
,src.ConvertedESTTimeSK                      
,src.LeadQuantity                            
,src.LeadAgeInSecondsQuantity                
,src.LeadIsConvertedQuantity                 
,src.LeadIsDoNotCallQuantity                 
,current_timestamp()--src.RecordInsertDateTime                  
,current_timestamp()--src.RecordUpdateDateTime                    
,src.RecordCheckSumNumber     
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Drop Integrate View

-- COMMAND ----------

Drop view integrate_rasflead 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Validation

-- COMMAND ----------

select Count(*) from gold.leadfact
