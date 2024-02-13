# Mount the Azure Data Lake Gen 2 - update 10-21-22 - 1341

dbutils.fs.mount( 
    source = "wasbs://raw@rcnptcusdsdl2.blob.core.windows.net/",
    mount_point = "/mnt",
    extra_configs = {"fs.azure.account.key.rcnptcusdsdl2.blob.core.windows.net":"nscaeBMvUavBDCqEolhli2+1Kpy1nRXxazLNKpBYZfurrpKtqs4WY+o4yMHBajE+ojJadbZhzH/Ce6X6WH71cw=="}
    )

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls('/mnt/')

# COMMAND ----------

""" %python
dbutils.fs.mount(source = 'wasbs://raw@rcnptcusdsdl2.blob.core.windows.net',
                 mount_point = '/mnt/lh',
                extra_configs = {'fs.azure.account.key.rcnptcusdsdl2.blob.core.windows.net' : 'nscaeBMvUavBDCqEolhli2+1Kpy1nRXxazLNKpBYZfurrpKtqs4WY+o4yMHBajE+ojJadbZhzH/Ce6X6WH71cw=='} ) """
