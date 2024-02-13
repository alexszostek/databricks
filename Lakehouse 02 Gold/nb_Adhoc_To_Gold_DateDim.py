# Databricks notebook source
# Importing packages	

import pyspark	
from pyspark.sql import SparkSession	
from pyspark.sql.window import Window	
from pyspark.sql.functions import rank, col	
from pyspark.sql.functions import row_number

# COMMAND ----------

# Read and create df from the respective file

df_Read=spark.read.csv("/mnt/Adhoc/*.csv",header=True)
df_Read.display()


# COMMAND ----------

# Create temporary view 
df_Read.createOrReplaceTempView("vw_DateDim")

# COMMAND ----------

# MAGIC %sql 
# MAGIC /* Display all views syntax */
# MAGIC SHOW VIEWS

# COMMAND ----------

# MAGIC  
# MAGIC %sql 
# MAGIC Describe  vw_DateDim

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC /* Display Data in Silver table */
# MAGIC 
# MAGIC select * from vw_DateDim

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Upsert Data into Silver layer */
# MAGIC 
# MAGIC MERGE INTO gold.DateDim AS TARGET
# MAGIC USING vw_DateDim AS SOURCE
# MAGIC ON TARGET.DateSK = SOURCE.DateSk
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (DateSK  ,	DayDate  ,	DayDateName  ,	FullDateName  ,	IsWeekdayFlag  ,	IsWeekendFlag  ,	IsBankHolidayFlag  ,	IsBusinessDayFlag  ,	IsBankBusinessDayFlag  ,	IsCorporateHolidayFlag  ,	IsFederalHolidayFlag  ,	WeekOfYearNumber  ,	PreviousDayDateSK  ,	PreviousDayDate  ,	NextDayDateSK  ,	NextDayDate  ,	BusinessDayDate  ,	BusinessDayDateSK  ,	PreviousBusinessDayDateSK  ,	PreviousBusinessDayDate  ,	NextBusinessDayDateSK  ,	NextBusinessDayDate  ,	SameDayWeekAgoDateSK  ,	SameDayWeekAgoDate  ,	SameDayMonthAgoDateSK  ,	SameDayMonthAgoDate  ,	SameDayYearAgoDateSK  ,	SameDayYearAgoDate  ,	IsFirstDayOfSundayStartWeekFlag  ,	IsFirstDayOfMondayStartWeekFlag  ,	IsFirstDayOfMonthFlag  ,	IsFirstDayOfQuarterFlag  ,	IsFirstDayOfYearFlag  ,	IsLastDayOfSundayStartWeekFlag  ,	IsLastDayOfMondayStartWeekFlag  ,	IsLastDayOfMonthFlag  ,	IsLastDayOfQuarterFlag  ,	IsLastDayOfYearFlag  ,	WeekSundayStartBeginDateSK  ,	WeekSundayStartBeginDate  ,	WeekSundayStartEndDateSK  ,	WeekSundayStartEndDate  ,	WeekSundayStartRangeName  ,	WeekSundayStartAbbrvRangeName  ,	WeekMondayStartBeginDateSK  ,	WeekMondayStartBeginDate  ,	WeekMondayStartEndDateSK  ,	WeekMondayStartEndDate  ,	WeekMondayStartRangeName  ,	WeekMondayStartAbbrvRangeName  ,	MonthBeginDateSK  ,	MonthBeginDate  ,	MonthEndDateSK  ,	MonthEndDate  ,	QuarterBeginDateSK  ,	QuarterBeginDate  ,	QuarterEndDateSK  ,	QuarterEndDate  ,	YearBeginDateSK  ,	YearBeginDate  ,	YearEndDateSK  ,	YearEndDate  ,	DayOfSundayStartWeekNumber  ,	DayOfMondayStartWeekNumber  ,	DayOfMonthNumber  ,	DayOfYearNumber  ,	MonthNumber  ,	QuarterNumber  ,	YearNumber  ,	DayName  ,	DayAbbrvName  ,	MonthSK  ,	MonthName  ,	MonthAbbrvName  ,	MonthAndYearName  ,	YearAndMonthName  ,	YearAndMonthAbbrvName  ,	MonthAbbrvAndYearName  ,	MonthAbbrvAndYearAbbrvName  ,	YearAbbrvAndMonthAbbrvName  ,	QuarterSK  ,	QuarterName  ,	QuarterAbbrvName  ,	QuarterAndYearName  ,	YearAndQuarterName  ,	YearAndQuarterAbbrvName  ,	QuarterAbbrvAndYearName  ,	QuarterAbbrvAndYearAbbrvName  ,	YearAbbrvAndQuarterAbbrvName  ,	YearSK  ,	YearName  ,	YearAbbrvName  ,	DayInEpochQuantity  ,	WeekInEpochQuantity  ,	MonthInEpochQuantity  ,	QuarterInEpochQuantity  ,	YearInEpochQuantity  ,	RecordEffectiveDateTime  ,	RecordExpirationDateTime  ,	RecordInsertDateTime  ,		RecordUpdateDateTime    
# MAGIC )
# MAGIC VALUES (DateSK  ,	DayDate  ,	DayDateName  ,	FullDateName  ,	IsWeekdayFlag  ,	IsWeekendFlag  ,	IsBankHolidayFlag  ,	IsBusinessDayFlag  ,	IsBankBusinessDayFlag  ,	IsCorporateHolidayFlag  ,	IsFederalHolidayFlag  ,	WeekOfYearNumber  ,	PreviousDayDateSK  ,	PreviousDayDate  ,	NextDayDateSK  ,	NextDayDate  ,	BusinessDayDate  ,	BusinessDayDateSK  ,	PreviousBusinessDayDateSK  ,	PreviousBusinessDayDate  ,	NextBusinessDayDateSK  ,	NextBusinessDayDate  ,	SameDayWeekAgoDateSK  ,	SameDayWeekAgoDate  ,	SameDayMonthAgoDateSK  ,	SameDayMonthAgoDate  ,	SameDayYearAgoDateSK  ,	SameDayYearAgoDate  ,	IsFirstDayOfSundayStartWeekFlag  ,	IsFirstDayOfMondayStartWeekFlag  ,	IsFirstDayOfMonthFlag  ,	IsFirstDayOfQuarterFlag  ,	IsFirstDayOfYearFlag  ,	IsLastDayOfSundayStartWeekFlag  ,	IsLastDayOfMondayStartWeekFlag  ,	IsLastDayOfMonthFlag  ,	IsLastDayOfQuarterFlag  ,	IsLastDayOfYearFlag  ,	WeekSundayStartBeginDateSK  ,	WeekSundayStartBeginDate  ,	WeekSundayStartEndDateSK  ,	WeekSundayStartEndDate  ,	WeekSundayStartRangeName  ,	WeekSundayStartAbbrvRangeName  ,	WeekMondayStartBeginDateSK  ,	WeekMondayStartBeginDate  ,	WeekMondayStartEndDateSK  ,	WeekMondayStartEndDate  ,	WeekMondayStartRangeName  ,	WeekMondayStartAbbrvRangeName  ,	MonthBeginDateSK  ,	MonthBeginDate  ,	MonthEndDateSK  ,	MonthEndDate  ,	QuarterBeginDateSK  ,	QuarterBeginDate  ,	QuarterEndDateSK  ,	QuarterEndDate  ,	YearBeginDateSK  ,	YearBeginDate  ,	YearEndDateSK  ,	YearEndDate  ,	DayOfSundayStartWeekNumber  ,	DayOfMondayStartWeekNumber  ,	DayOfMonthNumber  ,	DayOfYearNumber  ,	MonthNumber  ,	QuarterNumber  ,	YearNumber  ,	DayName  ,	DayAbbrvName  ,	MonthSK  ,	MonthName  ,	MonthAbbrvName  ,	MonthAndYearName  ,	YearAndMonthName  ,	YearAndMonthAbbrvName  ,	MonthAbbrvAndYearName  ,	MonthAbbrvAndYearAbbrvName  ,	YearAbbrvAndMonthAbbrvName  ,	QuarterSK  ,	QuarterName  ,	QuarterAbbrvName  ,	QuarterAndYearName  ,	YearAndQuarterName  ,	YearAndQuarterAbbrvName  ,	QuarterAbbrvAndYearName  ,	QuarterAbbrvAndYearAbbrvName  ,	YearAbbrvAndQuarterAbbrvName  ,	YearSK  ,	YearName  ,	YearAbbrvName  ,	DayInEpochQuantity  ,	WeekInEpochQuantity  ,	MonthInEpochQuantity  ,	QuarterInEpochQuantity  ,	YearInEpochQuantity  ,	current_timestamp()  ,	current_timestamp()  ,	current_timestamp()  ,	current_timestamp()  
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC Select DayInEpochQuantity  ,
# MAGIC WeekInEpochQuantity  ,
# MAGIC MonthInEpochQuantity  ,
# MAGIC QuarterInEpochQuantity  ,
# MAGIC YearInEpochQuantity  ,
# MAGIC IsCurrentRecordInd  ,
# MAGIC IsSourceRecordDeletedInd  ,
# MAGIC RecordEffectiveDateTime  ,
# MAGIC RecordExpirationDateTime  ,
# MAGIC ETLInsertBatchID  ,
# MAGIC RecordInsertDateTime  ,
# MAGIC RecordInsertUserName  ,
# MAGIC ETLUpdateBatchID  ,
# MAGIC RecordUpdateDateTime  ,
# MAGIC RecordUpdateUserName  ,
# MAGIC RecordCheckSumNumber  ,* from gold.DateDim

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Please truncate the table in case you want to reload the data and follow above process. Insert into Select statement will not work because of mismatching datatype with the file.
