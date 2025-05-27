from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='trigger_databricks_batch_inference',
    default_args=default_args,
    description='Trigger a batch inference Databricks job from Airflow',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['databricks', 'inference'],
) as dag:

    run_databricks_job = DatabricksRunNowOperator(
        task_id='run_inference_pipeline',
        databricks_conn_id='databricks_default',  # Defined in Airflow connections
        job_id='35216062022639'  # Replace this with your actual Databricks job ID in string format
        '''
        # Optional: Pass parameters to the Databricks for either the 
        # Python or Notebook task type.
        notebook_params={
            "model_uri": "models:/credit_risk_model/Production",
            "input_path": "/mnt/input_data/test.csv",
            "output_path": "/mnt/output_data/predictions.csv"
        }
        '''
    )
