# Databricks notebook source
# PULL LIB
# OpportunityFieldHistory


import numpy as np
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
configs = {
    "fs.adl.oauth2.access.token.provider.type": "CustomAccessTokenProvider",
    "fs.adl.oauth2.access.token.custom.provider": spark.conf.get("spark.databricks.passthrough.adls.tokenProviderClassName")
  }


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

fslsSchema = StructType(
    [
        StructField("path", StringType()),
        StructField("name", StringType()),
        StructField("size", StringType()),
        StructField("modtime", StringType()),
    ]
)

filelist = dbutils.fs.ls(
    "/mnt/raw/" + source_system + "/" + source_object + "/Processing"
)
df_files = spark.createDataFrame(filelist, fslsSchema)
d_array = np.array(df_files.select("name").collect())
# display(d_array)

# COMMAND ----------

for d in d_array:
    rootFile = d[0]
    SourceFile = (
        "dbfs:/mnt/raw/"
        + source_system
        + "/"
        + source_object
        + "/Processing/"
        + rootFile
    )
    dbutils.fs.rm(SourceFile)
    print(SourceFile)

# COMMAND ----------


