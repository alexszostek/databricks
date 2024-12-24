# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Variables
metastore_id = "fb73768a-06ee-4518-aa9f-89f5d17175c6"
warehouse_id = "e045961a518c99ae"
workspace_id = '2997204634952446'
account_id = '60079c41-9423-4657-85d3-f243bba4664f'
schema_name = "query"
catalog_name = 'rocket_mortgage_catalog_prod'
host = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}

# # API Request Body
body = {
    "owner": "UG-RocketMortgage-Data-Core-L1"
}

# API Request
r = requests.patch(
    f"{host}/api/2.1/unity-catalog/catalogs/{catalog_name}",
    headers=headers,
    json=body
).json()

response_json = r

# df = pd.DataFrame(response_json)

display(response_json)

# COMMAND ----------

# Variables
metastore_id = "fb73768a-06ee-4518-aa9f-89f5d17175c6"
warehouse_id = "e045961a518c99ae"
workspace_id = '2997204634952446'
account_id = '60079c41-9423-4657-85d3-f243bba4664f'
schema_name = "query"
catalog_name = 'rocket_mortgage_catalog_build'
host = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}

# # API Request Body
body = {
    "owner": "UG-RocketMortgage-Data-Core-L1"
}

# API Request
r = requests.patch(
    f"{host}/api/2.1/unity-catalog/catalogs/{catalog_name}",
    headers=headers,
    json=body
).json()

response_json = r

# df = pd.DataFrame(response_json)

display(response_json)
