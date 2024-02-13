# Databricks notebook source
# Import the required module
from pyspark.sql import SparkSession

# Create or get the Spark session
spark = SparkSession.builder.appName("DropdownWidgetFromQuery").getOrCreate()

# Execute the query to get distinct device names
query_device = """
SELECT distinct device 
FROM hive_metastore.dbacademy_szostek2009_gmail_com_spark_programming_asp_1_2___databricks_platform.events
"""
devices_df = spark.sql(query_device)

# Collect the results as a list of strings
devices_list = [row['device'] for row in devices_df.collect()]

# Create the dropdown widget with the list of devices
dbutils.widgets.dropdown("Device", devices_list[0], devices_list)



# COMMAND ----------

# Execute the query to get distinct traffic_source names
query_traffic_source = """
SELECT distinct traffic_source
FROM hive_metastore.dbacademy_szostek2009_gmail_com_spark_programming_asp_1_2___databricks_platform.events
"""
devices_df = spark.sql(query_traffic_source)

# Collect the results as a list of strings
list = [row['traffic_source'] for row in devices_df.collect()]

# Create the dropdown widget with the list of devices
dbutils.widgets.dropdown("Traffic Source", list[0], list)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT traffic_source, event_name, device
# MAGIC FROM hive_metastore.dbacademy_szostek2009_gmail_com_spark_programming_asp_1_2___databricks_platform.events

# COMMAND ----------

from datetime import datetime, timedelta

# Input string representing the month and year
input_month_year = "Jan-2020"

# Parse the string into a datetime object for the first day of the month
start_of_month = datetime.strptime(input_month_year, "%b-%Y")

# Calculate the end of the month by moving to the first day of the next month and then subtracting a day
end_of_month = start_of_month + timedelta(days=32)
end_of_month = end_of_month.replace(day=1) - timedelta(days=1)

# Convert the start of the month to Unix timestamp in microseconds
start_timestamp_microseconds = int(start_of_month.timestamp() * 1e6)

# Convert the end of the month to Unix timestamp in microseconds, ensuring to cover the entire last day
# by setting the time to the very end of the day (23:59:59.999999)
end_of_day = end_of_month.replace(hour=23, minute=59, second=59, microsecond=999999)
end_timestamp_microseconds = int(end_of_day.timestamp() * 1e6)

print(f"Start of Month: {start_timestamp_microseconds}")
print(f"End of Month: {end_timestamp_microseconds}")


# COMMAND ----------

from datetime import datetime, timedelta

# Input string representing the month and year
input_month_year = dbutils.widgets.get("Timeframe")

# Parse the string into a datetime object for the first day of the month
start_of_month = datetime.strptime(input_month_year, "%b-%Y")

# Calculate the end of the month by moving to the first day of the next month and then subtracting a day
end_of_month = start_of_month + timedelta(days=32)
end_of_month = end_of_month.replace(day=1) - timedelta(days=1)

# Convert the start of the month to Unix timestamp in microseconds
start_timestamp_microseconds = int(start_of_month.timestamp() * 1e6)

# Convert the end of the month to Unix timestamp in microseconds, ensuring to cover the entire last day
# by setting the time to the very end of the day (23:59:59.999999)
end_of_day = end_of_month.replace(hour=23, minute=59, second=59, microsecond=999999)
end_timestamp_microseconds = int(end_of_day.timestamp() * 1e6)

# print(f"Start of Month: {start_timestamp_microseconds}")
# print(f"End of Month: {end_timestamp_microseconds}")



# Get the value from the widget
wdg_device = dbutils.widgets.get("Device")
wdg_traffic_source = dbutils.widgets.get("Traffic Source")


# Now, you can use `selected_state` in your operations
# For example, using it in a query
query = f"""
SELECT *
FROM hive_metastore.dbacademy_szostek2009_gmail_com_spark_programming_asp_1_2___databricks_platform.events
where traffic_source = '{wdg_traffic_source}'
and device = '{wdg_device}'
"""

# Execute the query using Spark SQL
df = spark.sql(query)

# Show the result
display(df)

