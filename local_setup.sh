#!/bin/bash
# https://adb-477018903746647.7.azuredatabricks.net
export AIRFLOW_HOME=$(pwd)
export AIRFLOW_CONN_DATABRICKS_DEFAULT=''
# Optional: Make sure example DAGs show up
export AIRFLOW__CORE__LOAD_EXAMPLES=True
# Optional: Prevent DAGs from being paused on creation
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

# Stop any running Airflow services
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Reset the database and recreate everything
airflow db reset -y 

# Initialize fresh metadata DB
airflow db init

# Create an admin user 
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Create the default connection for Databricks in the airflow.db
airflow connections add databricks_default \
  --conn-type databricks \
  --conn-host '' \
  --conn-extra ''


echo "Airflow environment initialized and admin user created."