-- Databricks notebook source
Use Silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Table RocketAutoSalesforceOpportunity DDL 

-- COMMAND ----------

Create table silver.RocketAutoSalesforceOpportunity (
Id        string ,
AccountId        string ,
Account_Id__c        string ,
Adobe_ID__c        string ,
Amount        decimal(38,18) ,
APCO_ID__c        string ,
AutoFi_ID__c        string ,
Automotive_Processor__c        string ,
Budget_Confirmed__c        boolean ,
Call_Campaign_Expiration_Date__c        timestamp ,
CampaignId        string ,
Client_Agrees__c        boolean ,
CloseDate        timestamp ,
ClosedDateAge__c        decimal(38,18) ,
Confirmation_Email_Sent__c        boolean ,
ContactId        string ,
ContractId        string ,
CreatedById        string ,
CreatedDate        timestamp ,
Create_DateTime__c        timestamp ,
Credit_App_Approved__c        boolean ,
Credit_App_Complete__c        boolean ,
CTIA_Consent__c        boolean ,
Dealer_Bill_of_Sale_Received__c        boolean ,
Dealer_Deposit__c        decimal(38,18) ,
Dealer_Id__c        string ,
Dealer_Name__c        string ,
Dealer_Policies__c        string ,
Dealer_State__c        string ,
Dealer_Success_Manager__c        string ,
Delivery_Notes__c        string ,
Description        string ,
Discovery_Completed__c        boolean ,
Down_Payment__c        decimal(38,18) ,
Email__c        string ,
ExpectedRevenue        decimal(38,18) ,
Finance_Type__c        string ,
First_Call_DateTime__c        timestamp ,
Fiscal        string ,
FiscalQuarter        int ,
FiscalYear        int ,
ForecastCategory        string ,
ForecastCategoryName        string ,
HasOpenActivity        boolean ,
HasOpportunityLineItem        boolean ,
HasOverdueTask        boolean ,
IsClosed        boolean ,
IsDeleted        boolean ,
IsPrivate        boolean ,
IsWon        boolean ,
Jornaya_Id__c        string ,
Keys_In_Hand__c        boolean ,
LastActivityDate        timestamp ,
LastAmountChangedHistoryId        string ,
LastCloseDateChangedHistoryId        string ,
LastModifiedById        string ,
LastModifiedDate        timestamp ,
LastReferencedDate        timestamp ,
LastStageChangeDate        timestamp ,
LastViewedDate        timestamp ,
Last_Called_By__c        string ,
Last_Call_DateTime__c        timestamp ,
Last_Call_Duration__c        string ,
Last_Call_Result__c        string ,
LeadSource        string ,
Lead_Buy_partner_name__c        string ,
Lead_Id__c        string ,
Lead_Type__c        string ,
Licensed_Status__c        string ,
Loss_Reason__c        string ,
Make__c        string ,
Model__c        string ,
Name        string ,
NextStep        string ,
Opportunity_Id__c        string ,
Opportunity_Record_ID__c        string ,
OwnerId        string ,
PartnerAccountId        string ,
Pickup_DateTime__c        timestamp ,
Pricebook2Id        string ,
Price__c        decimal(38,18) ,
Probability        decimal(38,18) ,
Purchase_Timeframe__c        string ,
PushCount        int ,
RecordTypeId        string ,
RecordTypeName__c        string ,
Referral_URL__c        string ,
ROI_Analysis_Completed__c        boolean ,
RouteOne_Jacket_ID__c        string ,
Schedule_Appointment__c        timestamp ,
Sell_Vehicle_Condition__c        string ,
Sell_Vehicle_Dealer_Quote__c        decimal(38,18) ,
Sell_Vehicle_Exterior_Color__c        string ,
Sell_Vehicle_History_Report__c        string ,
Sell_Vehicle_Make__c        string ,
Sell_Vehicle_Mileage__c        decimal(38,18) ,
Sell_Vehicle_Model__c        string ,
Sell_Vehicle_Payoff_Amount__c        decimal(38,18) ,
Sell_Vehicle_Trim__c        string ,
Sell_Vehicle_Value__c        decimal(38,18) ,
Sell_Vehicle_VIN__c        string ,
Sell_Vehicle_Year__c        string ,
Sent_to_Dealer_by__c        string ,
Sent_to_Dealer_DateTime__c        timestamp ,
Service_Model__c        string ,
SourceId        string ,
StageName        string ,
SyncedQuoteId        string ,
SystemModstamp        timestamp ,
Target_Payment__c        decimal(38,18) ,
TCPA_Consent__c        string ,
Title_Copy_Received__c        boolean ,
TotalOpportunityQuantity        decimal(38,18) ,
Trade_In__c        boolean ,
Trim__c        string ,
Type        string ,
Vehicle_Availability_Verified__c        boolean ,
Vehicle_Detail_Page__c        string ,
Vehicle_Exterior_Color__c        string ,
Vehicle_History_Report__c        string ,
Vehicle_Id__c        decimal(38,18) ,
Vehicle_Listing__c        string ,
Vehicle_Make__c        string ,
Vehicle_Mileage__c        decimal(38,18) ,
Vehicle_Model__c        string ,
Vehicle_Price__c        decimal(38,18) ,
Vehicle_Total_Fees__c        decimal(38,18) ,
Vehicle_Trim__c        string ,
Vehicle_Type__c        string ,
Vehicle_VIN__c        string ,
Vehicle_Year__c        string ,
Vehicle__c        string ,
VIN__c        string ,
X1st_Alternative_Vehicle_VIN__c        string ,
X2nd_Alternative_Vehicle_VIN__c        string ,
Year__c        string ,
RecordChecksumNumber string,
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

--Alter table Silver..RocketAutoSalesforceOpportunity
--Add columns 
--(test string) 

