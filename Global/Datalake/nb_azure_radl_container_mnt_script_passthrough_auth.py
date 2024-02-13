# Find Current Environment 

beta = 'rabetads01eus2adlssa'
uat = 'rauatds01eus2adlssa'
prod = 'raprodds01eus2adlssa'

env = ''

cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
l_cluster_name = cluster_name.lower()

if "beta" in l_cluster_name:
    env = beta
elif "uat" in l_cluster_name:
    env = uat
elif "prod" in l_cluster_name:
    env = prod

print(env)


# List Available Mount Points

dbutils.fs.mounts()

# unmount a point

# dbutils.fs.unmount("dbfs:/mnt/data/")


containers = ["bronze", "copper", "gold", "platinum", "raw", "silver", "tin"] # replace with your list of containers values

configs = {"fs.azure.account.auth.type": "CustomAccessToken", "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")}

for container in containers:
    print(container)
    try:
        dbutils.fs.mount(
            source="abfss://" + container + "@" + env + ".dfs.core.windows.net/",
            mount_point="/mnt/" + container + "/",
            extra_configs=configs)
        print(f"Mount successful for {env}!")
    except Exception as e:
        print(f"Mount failed for {env} with error: {e}")
