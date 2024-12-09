# Databricks notebook source
import requests
import json

# Variables
metastore_id = "fb73768a-06ee-4518-aa9f-89f5d17175c6"
warehouse_id = "e045961a518c99ae"
schema_name = "query"
host = "https://" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer " + dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}

# COMMAND ----------

import requests
import json

# Databricks API endpoint
url = "https://dbc-4ab4ee71-fa38.cloud.databricks.com/api/2.1/jobs/list"


# Send the GET request
response = requests.get(url, headers=headers)

# Print the response
if response.status_code == 200:
    print("Current Jobs Deployed.")
    display(response.json())
else:
    print(f"Faield to list job. Status code: {response.status_code}")
    print(response.text)


# COMMAND ----------

import requests
import json

# Databricks API endpoint
url = "https://dbc-4ab4ee71-fa38.cloud.databricks.com/api/2.1/jobs/create"

# Job payload
payload = {
  "name": "rocket-mortgage-stage-1-tpch",
  "email_notifications": {
    "no_alert_for_skipped_runs": False
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "customer",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "c_custkey"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "lineitem",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "l_linenumber",
          "source_object": "lineitem"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "nation",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "n_nationkey",
          "source_object": "nation"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "orders",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "o_orderkey",
          "source_object": "orders"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "part",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "p_partkey",
          "source_object": "part"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "partsupp",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "ps_partkey",
          "source_object": "partsupp"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "region",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "r_regionkey",
          "source_object": "region"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "supplier",
      "run_if": "ALL_SUCCESS",
      "run_job_task": {
        "job_id": 286921488638272,
        "job_parameters": {
          "primary_key": "s_suppkey",
          "source_object": "supplier"
        }
      },
      "timeout_seconds": 900,
      "health": {
        "rules": [
          {
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": 600
          }
        ]
      },
      "email_notifications": {},
      "webhook_notifications": {}
    }
  ],
  "queue": {
    "enabled": True
  },
  "run_as": {
    "service_principal_name": "a15ac0b5-d75d-41e3-a468-1ae848f8d349"
  }
}

# Send the POST request
response = requests.post(url, headers=headers, data=json.dumps(payload))

# Print the response
if response.status_code == 200:
    print("Job created successfully.")
    print(response.json())
else:
    print(f"Failed to create job. Status code: {response.status_code}")
    print(response.text)


# COMMAND ----------

import requests
import json

# Databricks API endpoint
url = "https://dbc-4ab4ee71-fa38.cloud.databricks.com/api/2.1/jobs/reset"

# Job payload
payload = {
  "job_id": 775407547060197,
  "new_settings": {
    "name": "rocket-mortgage-stage-1-tpch",
    "email_notifications": {
      "no_alert_for_skipped_runs": False
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
      {
        "task_key": "customer",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "c_custkey"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      },
      {
        "task_key": "lineitem",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "l_linenumber",
            "source_object": "lineitem"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      },
      {
        "task_key": "nation",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "n_nationkey",
            "source_object": "nation"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      },
      {
        "task_key": "orders",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "o_orderkey",
            "source_object": "orders"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      },
      {
        "task_key": "part",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "p_partkey",
            "source_object": "part"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      },
      {
        "task_key": "partsupp",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "ps_partkey",
            "source_object": "partsupp"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      },
      {
        "task_key": "region",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "r_regionkey",
            "source_object": "region"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      },
      {
        "task_key": "supplier",
        "run_if": "ALL_SUCCESS",
        "run_job_task": {
          "job_id": 286921488638272,
          "job_parameters": {
            "primary_key": "s_suppkey",
            "source_object": "supplier"
          }
        },
        "timeout_seconds": 900,
        "health": {
          "rules": [
            {
              "metric": "RUN_DURATION_SECONDS",
              "op": "GREATER_THAN",
              "value": 600
            }
          ]
        },
        "email_notifications": {},
        "webhook_notifications": {}
      }
    ],
    "queue": {
      "enabled": True
    },
    "run_as": {
      "service_principal_name": "a15ac0b5-d75d-41e3-a468-1ae848f8d349"
    }
  }
}


# Send the POST request
response = requests.post(url, headers=headers, data=json.dumps(payload))

# Print the response
if response.status_code == 200:
    print("Job created successfully.")
    print(response.json())
else:
    print(f"Failed to create job. Status code: {response.status_code}")
    print(response.text)


# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   `system`.lakeflow.jobs
# MAGIC where
# MAGIC   delete_time is null
# MAGIC order by
# MAGIC   change_time desc
