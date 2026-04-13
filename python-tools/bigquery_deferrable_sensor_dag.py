from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_bq_job_status(ti, **context):
    """
    Python callable for the PythonSensor to check BigQuery job status.
    """
    # Pull the job ID from the BigQuery task
    job_id = ti.xcom_pull(task_ids='run_bq_job')
    logging.info(f"Checking status for BigQuery job: {job_id}")
    
    if not job_id:
        logging.error("No job ID found in XCom. Make sure the upstream task returned it.")
        return False
        
    hook = BigQueryHook()
    try:
        # Note: In some Airflow versions, get_job might require location or project_id
        # If you encounter issues, try: hook.get_job(job_id=job_id, project_id='your_project', location='your_location')
        job = hook.get_job(job_id=job_id)
        logging.info(f"Job state: {job.state}")
        
        if job.state == 'DONE':
            if job.error_result:
                raise Exception(f"Job failed with error: {job.error_result}")
            return True
        return False
    except Exception as e:
        logging.error(f"Error checking job status: {e}")
        return False

def print_final_state(ti, **context):
    """
    Print the final state results.
    """
    job_id = ti.xcom_pull(task_ids='run_bq_job')
    logging.info(f"===== BigQuery Job {job_id} completed successfully. =====")
    print(f"===== Final State: SUCCESS for job {job_id} =====")

with DAG(
    'bigquery_deferrable_sensor_sample',
    default_args=default_args,
    description='A sample DAG running BigQuery job in deferrable mode and monitoring with a sensor',
    schedule_interval=None,
    catchup=False,
    tags=['example', 'bigquery', 'deferrable'],
) as dag:

    # 1. Run a BigQuery job in deferrable mode
    # The operator will submit the job and then defer to the triggerer,
    # freeing up the worker slot while the job runs.
    run_bq_job = BigQueryInsertJobOperator(
        task_id='run_bq_job',
        configuration={
            "query": {
                "query": "SELECT 1 as test_value;",
                "useLegacySql": False,
            }
        },
        deferrable=True,
    )

    # 2. Create a Python sensor to monitor the job status
    # Since the operator above is deferrable, it will wait for the job to complete
    # before this sensor runs. This sensor demonstrates how to check status
    # of a job by ID if needed in a separate step.
    monitor_job = PythonSensor(
        task_id='monitor_job',
        python_callable=check_bq_job_status,
        poke_interval=10,
        timeout=60,
        mode='poke',
    )

    # 3. Print the states results in the end
    print_results = PythonOperator(
        task_id='print_results',
        python_callable=print_final_state,
    )

    run_bq_job >> monitor_job >> print_results
