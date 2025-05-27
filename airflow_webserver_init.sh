#!/bin/bash
pkill -f "airflow webserver"
export AIRFLOW_HOME=$(pwd)
airflow webserver