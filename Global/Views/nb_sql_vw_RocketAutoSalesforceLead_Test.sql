-- Databricks notebook source
use silver

-- COMMAND ----------


CREATE OR REPLACE TABLE silver.RocketAutoSalesforceLead_Test (
          Adobe_ID__c string,
          Affliate__c boolean,
          After_Market_Exhaust__c string,
          After_Market_Other__c string,
          After_Market_Performance__c string,
          After_Market_Sterio__c string,
          After_Market_Suspension__c string,
          After_Market_Wheels__c string,
          AnnualRevenue decimal(38,18),
          AutoFI__c string,
          Black_Book_Adobe_ID__c string,
          Black_Book_Client_Stage__c string,
          Black_Book_Session_ID__c string,
          Black_Book_Site_Variant__c string,
          Black_Book_Valuation_High__c string,
          Black_Book_Valuation_Low__c string,
          City string,
          Company string,
          ConvertedAccountId string,
          ConvertedContactId string,
          ConvertedDate timestamp,
          ConvertedOpportunityId string,
          Country string,
          CreatedById string,
          CreatedDate timestamp,
          Dealer_Id__c string,
          Dealer_Lead_Title__c string,
          Description string,
          Does_It_Run__c string,
          DoNotCall boolean,
          Email string,
          EmailBouncedDate timestamp,
          EmailBouncedReason string,
          et4ae5__HasOptedOutOfMobile__c boolean,
          et4ae5__Mobile_Country_Code__c string,
          Exterior_Color__c string,
          Exterior_Condition__c string,
          Fax string,
          Features__c string,
          FirstName string,
          Flood_Fire_Damage__c string,
          GeocodeAccuracy string,
          Had_Accident__c string,
          Hail_Damage__c string,
          HasOptedOutOfEmail boolean,
          HasOptedOutOfFax boolean,
          Id string,
          Industry string,
          Interior_Condition__c string,
          IsConverted boolean,
          IsDeleted boolean,
          IsUnreadByOwner boolean,
          Jigsaw string,
          JigsawContactId string,
          Jornaya_Id__c string,
          Keys__c string,
          LastActivityDate timestamp,
          LastModifiedById string,
          LastModifiedDate timestamp,
          LastName string,
          LastReferencedDate timestamp,
          LastTransferDate timestamp,
          LastViewedDate timestamp,
          Latitude decimal(38,18),
          LeadSource string,
          Lead_Buy_partner_name__c string,
          Lead_Id__c string,
          Loan_Application_Id__c string,
          Longitude decimal(38,18),
          MasterRecordId string,
          Mechanical_Condition__c string,
          MiddleName string,
          Miles_On_Tires__c string,
          MobilePhone string,
          Name string,
          NumberOfEmployees int,
          Number_of_Roof_Tops__c string,
          OwnerId string,
          PartnerAccountId string,
          Phone string,
          PhotoUrl string,
          PostalCode string,
          Rating string,
          RecordTypeId string,
          Referral_URL__c string,
          Rocket_Auto_Client_Id__c string,
          Rocket_Person_ID__c string,
          Salutation string,
          Seats__c string,
          Sell_Price__c decimal(38,18),
          Smoked_In__c string,
          State string,
          Status string,
          Stock_Number__c string,
          Street string,
          Suffix string,
          SystemModstamp timestamp,
          Title string,
          Title__c string,
          Trade_In__c boolean,
          UserID__c string,
          Vehicle_Id__c decimal(38,18),
          Vehicle_Make__c string,
          Vehicle_Mileage__c decimal(38,18),
          Vehicle_Model__c string,
          Vehicle_Price__c decimal(38,18),
          Vehicle_Status__c string,
          Vehicle_Trim__c string,
          Vehicle_Type__c string,
          Vehicle_VIN__c string,
          Vehicle_Year__c string,
          Warning_Lights__c string,
          Website string,
          RecordChecksumNumber string,
          ETLInsertBatchID string GENERATED ALWAYS AS ('-1') ,
          RecordInsertDateTime string ,
          RecordInsertUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String)) ,
          ETLUpdateBatchID string GENERATED ALWAYS AS ('-1') ,
          RecordUpdateDateTime string  ,
          RecordUpdateUserName string GENERATED ALWAYS AS (CAST('Azure_Dbrics_User' AS String))  
)

 

-- COMMAND ----------

Optimize silver.RocketAutoSalesforceLead_Test 
