import datetime as dt
import json
import logging
import sys
import time
import yaml
import shutil
from pathlib import Path

import import_helper
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datadog_helper import DataDogHelper
from dbt_operator import DbtHelper
from sdp_airflow_dag import DAG

glue_schema_registry = import_helper.load("schema_registry.py")

DEFAULT_ARGS = {
    "owner": "infra",
    "depends_on_past": False,
    "retries": 0,
    "pool": "tdm-credit-card",
    "start_date": days_ago(1),
    "catchup": False,
}

"""
To run the dag for a single table use:
{
"table_name": <table_name>
}
To trigger a full-refresh for a single table use:
{
"table_name": <table_name>,
  "dbt": {
    "run": {
      "full-refresh": true
    }
  }
}
"""

with DAG(
    dag_id="tdm.raw_to_cleansed__tier2",
    default_args=DEFAULT_ARGS,
    tags=["tdm-credit-card"],
    schedule_interval="*/60 * * * *",
) as dag:
    datadog = DataDogHelper(dag)
    errors = []

    def metric_name(name):
        return f"tdm.micb.warehouse.pipeline.airflow.{name}"

    def handle_dbt_run_results(dbt_path, datadog=datadog):
        """Handle the results of a dbt run

        This will look for the run_results.json file and then send metrics to DataDog based on its contents.
        """

        try:
            f = open(f"{dbt_path}/target/run_results.json")
            run_results = json.load(f)
            logging.info(f"Processing run_results.json: {run_results}")
            if "results" in run_results:
                timestamp = time.time()
                for result in run_results["results"]:
                    dbt_id = result["unique_id"]
                    dbt_status = result["status"]
                    dbt_rows_affected = (
                        0
                        if "rows_affected" not in result["adapter_response"]
                        else result["adapter_response"]["rows_affected"]
                    )

                    tags = [
                        "tdm:tdm-credit-card",
                        "dag:tdm-credit-card.raw_to_cleansed",
                        f"dbt_id:{dbt_id}",
                        f"dbt_status:{dbt_status}",
                    ]

                    datadog.Metric.send(
                        metric=metric_name("dbt_task_time_seconds"),
                        tags=tags,
                        points=[(timestamp, result["execution_time"])],
                        type="count",
                    )
                    datadog.Metric.send(
                        metric=metric_name("dbt_rows_affected"),
                        tags=tags,
                        points=[(timestamp, dbt_rows_affected)],
                        type="count",
                    )
        except BaseException as err:
            logging.error(
                f"Failed to parse dbt run_results.json and emit metrics: {err}"
            )
        else:
            f.close()

    def pipeline_dbt_run(dag, **context):
        """Pipeline dbt run

        This run will pull any existing glue schemas from glue schema registry and then generate dbt model files (note: will not replace existing models).

        This will also set up snowpipe objects for any existing glue schema.

        Afterwards, this will do a dbt run of all models in 'models/cleansed'. Note: this includes both generated models and existing models committed to the tdm repo.
        """
        with DbtHelper(dag, context) as dbt:
            logger = logging.getLogger("airflow.task")
            table_name = None
            item_per_chunk = 5

            if 'table_name' in context['dag_run'].conf:
                table_name = context['dag_run'].conf['table_name']
                item_per_chunk = 1

            schemaRegistry = glue_schema_registry.SchemaRegistry(
                registry_arn="registry-arn",
                assume_role_arn="assume-role-arn",
                assume_role_session_name="production.credit-card.airflow.raw_to_cleansed_dag.py",
                table_name=table_name
            )

            cnt_schemas = len(schemaRegistry.schema_definitions)

            # report DD metrics on number of schemas found
            logging.info(f"found {cnt_schemas} schemas")
            if table_name is not None:
                logging.info(f"processing {table_name} schema")

            datadog.Metric.send(
                metric=metric_name("schema_count"),
                tags=["tdm:tdm-credit-card", "dag:tdm.raw_to_cleansed"],
                points=[(time.time(), cnt_schemas)],
                type="count",
            )

            dbt_path = "{0}/dbt".format(dbt.env["DBT_TMPDIR"])
            if cnt_schemas > 0:
                dbt_vars = schemaRegistry.get_dbt_vars()

                # TODO need to write some tests (INV-32053)
                # dbt.test(vars=dbt_vars)
                dbt_yaml_vars = yaml.safe_load(dbt_vars)
                schema_tables = schemaRegistry.get_tables()

                zipped_vars_tables = list(zip(dbt_yaml_vars.items(), schema_tables))

                def chunk_lists(l, chunks=1):
                    length = len(l)
                    return [
                        l[i * length // chunks : (i + 1) * length // chunks]
                        for i in range(chunks)
                    ]

                def format_dbt_var(dbt_var):
                    name = dbt_var[0]
                    version = dbt_var[1]["version"]
                    raw_config = dbt_var[1]["raw_config"]
                    tables = dbt_var[1]["data_sources"]
                    columns = dbt_var[1]["columns"]
                    return f"{name}: {{version: {version}, raw_config: {raw_config}, data_sources: {tables}, columns: {columns}}}"

                for chunk in chunk_lists(zipped_vars_tables, item_per_chunk):
                    unzipped = list(zip(*chunk))
                    schema_registry_chunk = [
                        x
                        for x in schemaRegistry.schema_definitions
                        if x.sanitized_schema_name in unzipped[1]
                    ]

                    results = schemaRegistry.create_dbt_model_files(
                        target_schema="cleansed",
                        tags=["cleansed", "models", "data_pipeline"],
                        dbt_path=dbt_path,
                        schema_subset=schema_registry_chunk,
                    )

                    # report DD metrics on number of models generated
                    datadog.Metric.send(
                        metric=metric_name("created_file_count"),
                        tags=[
                            "tdm:tdm",
                            "dag:tdm.raw_to_cleansed",
                        ],
                        points=[(time.time(), results["count_created_files"])],
                        type="count",
                    )
                    datadog.Metric.send(
                        metric=metric_name("skipped_file_count"),
                        tags=[
                            "tdm:tdm",
                            "dag:tdm.raw_to_cleansed",
                        ],
                        points=[(time.time(), results["count_skipped_files"])],
                        type="count",
                    )

                    ## load vars into dbt project file
                    schemaRegistry.update_dbt_project_file_with_vars(dbt_path)

                    snowpipe_args = {
                        "storage_integration": "tdm",
                        "s3_bucket": "",
                        "aws_sns_topic": "",
                        "data_source": "BDP",
                        "names": unzipped[1],
                    }

                    formatted_dbt_vars_chunk = "{{{0}}}".format(
                        ",".join(format_dbt_var(d) for d in unzipped[0])
                    )

                    dbt.run_operation(
                        "create_snowpipe",
                        args=snowpipe_args,
                        vars=formatted_dbt_vars_chunk,
                    )

                    try:
                        dbt.run(
                            select=" ".join(
                                f"models/cleansed/{table_name}_cleansed.sql"
                                for table_name in unzipped[1]
                            ),
                            vars=formatted_dbt_vars_chunk,
                        )
                    except AirflowException as e:
                        errors.append(e)
                    finally:
                        handle_dbt_run_results(dbt_path=dbt_path)
                        shutil.rmtree(f"{dbt_path}/models/cleansed")
                        Path(f"{dbt_path}/models/cleansed").mkdir(
                            parents=True, exist_ok=True
                        )
                if len(errors) > 0:
                    for e in errors:
                        logger.error(f"Caught error during partial dbt run: {e}")
                    raise AirflowException("dbt failed. See error logs.")

            else:
                # Don't bother with any model generation stuff if there's no schemas found, just run whatever models are found in the dbt project repo
                try:
                    dbt.run(select="models/cleansed")
                finally:
                    handle_dbt_run_results(dbt_path=dbt_path)

    def legacy_dbt_run(dag, **context):
        """dbt run excluding any pipeline logic

        Note: This doesn't have a long term use case, but created to give an easier way to switch behavior back and forth during initial testing/startup (clee 6/9/2022)
        """
        with DbtHelper(dag, context) as dbt:
            try:
                dbt.run(select="models/cleansed")
            finally:
                handle_dbt_run_results(dbt_path="{0}/dbt".format(dbt.env["DBT_TMPDIR"]))

    # commented out for now, may utilize this later for triggering a dag run after this dag is complete
    trigger_dag_cc_epd_modeled = TriggerDagRunOperator(
        dag=dag,
        task_id="tdm",
        trigger_dag_id="tdm",
    )

    dbt_helper_task = PythonOperator(
        task_id="pipeline_dbt_run", python_callable=pipeline_dbt_run
    )
    # dbt_helper_task = PythonOperator(task_id="legacy_dbt_run", python_callable=legacy_dbt_run)

    dbt_helper_task >> trigger_dag_cc_epd_modeled
