# Databricks notebook source
# PULL LIB
# dbfs:/mnt/bronze/rasf/OpportunityFieldHistory

import numpy as np
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
##configs = {"fs.azure.account.auth.type": "CustomAccessToken", "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create input parameters

# COMMAND ----------

dbutils.widgets.text(
    "SourceSystem",
    "",
    "Root Source System",
)

dbutils.widgets.text(
    "SourceObject",
    "",
    "Root Source Object",
)



source_system = dbutils.widgets.get("SourceSystem")
source_object = dbutils.widgets.get("SourceObject")

# COMMAND ----------

# MAGIC %md
# MAGIC Construct File List from data frame | grab all files in queued folder at this point in time

# COMMAND ----------

fslsSchema = StructType(
    [
        StructField("path", StringType()),
        StructField("name", StringType()),
        StructField("size", StringType()),
        StructField("modtime", StringType()),
    ]
)

filelist = dbutils.fs.ls(
    "/mnt/raw/" + source_system + "/" + source_object + "/Queued"
)
df_files = spark.createDataFrame(filelist, fslsSchema)
d_array = np.array(df_files.select("name").collect())
# display(d_array)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC iterate through files in queued and move them to processsing folder

# COMMAND ----------

for d in d_array:
    rootFile = d[0]
    SourceFile = (
        "dbfs:/mnt/raw/"
        + source_system
        + "/"
        + source_object
        + "/Queued/"
        + rootFile
    )
    TargetFile = (
        "dbfs:/mnt/raw/"
        + source_system
        + "/"
        + source_object
        + "/Processing/"
        + rootFile
    )
    dbutils.fs.mv(SourceFile, TargetFile, recurse=True)
    print(SourceFile)
    print(TargetFile)
