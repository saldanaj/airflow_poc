from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime
import requests
import logging

TAG_KEY = "job_type"
TAG_VALUE = "ml_batch_inference"
DATABRICKS_INSTANCE = Variable.get("DATABRICKS_INSTANCE")  
DATABRICKS_TOKEN = Variable.get("DATABRICKS_TOKEN")

@dag(
    dag_id="trigger_tagged_databricks_jobs",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["databricks", "dynamic-mapping"],
)
def trigger_tagged_databricks_jobs():
    @task()
    def get_matching_job_ids():
        url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list"
        headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
        logging.info(f"Fetching jobs from Databricks: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        jobs = response.json().get("jobs", [])
        logging.info(f"Total jobs fetched: {len(jobs)}")
        matching_jobs = [
            job for job in jobs
            if job.get("settings", {}).get("tags", {}).get(TAG_KEY) == TAG_VALUE
        ]
        logging.info(f"Jobs matching tag {TAG_KEY}={TAG_VALUE}: {len(matching_jobs)}")
        for job in matching_jobs:
            logging.info(f"Matched Job: {job.get('job_id')} - {job.get('settings', {}).get('name')}")
        job_ids = [job["job_id"] for job in matching_jobs]
        return job_ids

    @task.branch()
    def check_any_jobs(job_ids):
        if job_ids:
            logging.info(f"Proceeding to trigger jobs: {job_ids}")
            return "trigger_databricks_job"
        else:
            logging.info("No jobs found to trigger.")
            return "finish"

    job_ids = get_matching_job_ids()
    branch = check_any_jobs(job_ids)

    trigger = DatabricksRunNowOperator.partial(
        task_id="trigger_databricks_job",
        databricks_conn_id="databricks_default",
    ).expand(job_id=job_ids)

    finish = EmptyOperator(task_id="finish")

    branch >> [trigger, finish]
    trigger >> finish

dag = trigger_tagged_databricks_jobs()