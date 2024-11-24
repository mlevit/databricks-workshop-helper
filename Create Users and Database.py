# Databricks notebook source
# DBTITLE 1,Databricks API Integration with User Details Setup
# Define the Databricks workspace URL and token
workspace_url = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)
access_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

current_user = (
    (dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())
    .split("@")[0]
    .replace(".", "_")
    .replace("+", "_")
)

# Define the headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

# Define the user details
users = [
    "first.last@company.com",
]

# COMMAND ----------

# DBTITLE 1,Automating User Creation with Databricks SCIM API
import requests
import json
from time import sleep

# Create users
for user in users:
    response = requests.post(
        f"{workspace_url}/api/2.0/preview/scim/v2/Users",
        headers=headers,
        data=json.dumps({"userName": user}),
    )

    if response.status_code == 201:
        print(f"User {user} created successfully.")
    else:
        if "already exists" not in response.text:
            print(f"Error creating user {user}: {response.text}")

    sleep(2)  # Sleep for 2 seconds

# COMMAND ----------

# DBTITLE 1,Automating User Schema and Table Creation
# Get the list of tables in the specified schema
tables_df = spark.sql("SHOW TABLES IN workshop.marat_levit_jhg_sql_24")

# Iterate over each user
for user in users:
    # Format the username to match the schema naming convention
    user = user.split("@")[0].replace(".", "_").replace("+", "_")

    # Create a schema for the user if it does not exist
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS workshop.{user}""")

    # Iterate over each table in the original schema
    for table in tables_df.collect():
        table_name = table["tableName"]

        # Clone the table into the user's schema
        clone_command = f"CREATE OR REPLACE TABLE workshop.{user}.{table_name} CLONE workshop.{current_user}.{table_name}"
        spark.sql(clone_command)
