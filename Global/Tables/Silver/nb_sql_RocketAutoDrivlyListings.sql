-- Databricks notebook source
use silver

-- COMMAND ----------


CREATE OR REPlACE TABLE Silver.RocketAutoDrivlyListings(
SnapshotTimestamp timestamp,
accidents    bigint ,
askingPrice    string ,
auction    string ,
autocheckScore    bigint ,
buyNowPrice    bigint ,
city    string ,
comments    string ,
condition    bigint ,
conditionReport    string ,
date    string ,
dealerCity    string ,
dealerState    string ,
description    string ,
distance    bigint ,
dom    bigint ,
doors    bigint ,
drivetrain    string ,
engine    string ,
exterior    string ,
facility    string ,
fuel    string ,
images    string ,
interior    string ,
lane    bigint ,
latitude    double ,
longitude    double ,
make    string ,
margin    bigint ,
marginpercent    double ,
mileage    bigint ,
minimumBid    bigint ,
model    string ,
owners    bigint ,
pickupLocation    string ,
price    bigint ,
primaryImage    string ,
retailId    string ,
retailValue    string ,
runNumber    bigint ,
saleDate    string ,
seats    bigint ,
seller    string ,
state    string ,
style    string ,
transmission    string ,
trim    string ,
type    string ,
url    string ,
vdp    string ,
vin    string ,
wholesaleValue    bigint ,
year    bigint ,
zip    string ,
RecordChecksumNumber string ,
ETLInsertBatchID string GENERATED ALWAYS AS ('-1') ,
RecordInsertDateTime timestamp ,
RecordInsertUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) ,
ETLUpdateBatchID string GENERATED ALWAYS AS ('-1') ,
RecordUpdateDateTime timestamp  ,
RecordUpdateUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))  
);

-- COMMAND ----------

--Alter table Silver.RocketAutoDrivlyListings
--Add columns 
--(test string) 

-- COMMAND ----------


