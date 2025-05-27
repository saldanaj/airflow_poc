#!/bin/bash

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
