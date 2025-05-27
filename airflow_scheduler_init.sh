#!/bin/bash
pkill -f "airflow scheduler"
export AIRFLOW_HOME=$(pwd)
airflow scheduler