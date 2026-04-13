from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys
import os

# Add current directory to path to import local module
# This is useful if python-tools is not in the python path
sys.path.append(os.path.dirname(__file__))
from external_task_bigquery_operator import ExternalTaskBigQuerySensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_done():
    logging.info("===== Combined Sensor + BigQuery Job finished! =====")

with DAG(
    'custom_operator_sample_dag',
    default_args=default_args,
    description='A sample DAG using a custom combined operator',
    schedule_interval=None,
    catchup=False,
    tags=['example', 'custom_operator'],
) as dag:

    # This task will first wait for 'some_external_dag'.'some_external_task'
    # and then run the BigQuery query.
    run_combined_job = ExternalTaskBigQuerySensor(
        task_id='run_combined_job',
        external_dag_id='some_external_dag',
        external_task_id='some_external_task',
        poke_interval=10,
        timeout=60,
        configuration={
            "query": {
                "query": "SELECT 1 as value;",
                "useLegacySql": False,
            }
        },
        deferrable=True, # This applies to the BigQuery part
    )

    print_task = PythonOperator(
        task_id='print_done',
        python_callable=print_done,
    )

    run_combined_job >> print_task
