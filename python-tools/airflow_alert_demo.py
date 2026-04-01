from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging

def custom_failure_callback(context):
    """
    Custom callback function to handle task failures and extract metadata.
    This function can be extended to send alerts to Slack, Teams, Email, etc.
    """
    ti = context.get('task_instance')
    dag = context.get('dag')
    
    # Extract fields
    task_id = ti.task_id if ti else "N/A"
    dag_id = ti.dag_id if ti else "N/A"
    
    # TaskInstance has an owner property (inherited from BaseOperator)
    owner = ti.owner if ti else "N/A"
    
    # Start and End Times
    start_time = ti.start_date.isoformat() if ti and ti.start_date else "N/A"
    
    # End time might not be set yet if we are in the failure callback of the running task.
    # We can use the current time as the end time for the purpose of the alert.
    end_time = ti.end_date.isoformat() if ti and ti.end_date else datetime.utcnow().isoformat()
    
    run_id = context.get('run_id', "N/A")
    
    # Duration calculation
    duration = ti.duration if ti and ti.duration else None
    if duration is None and ti and ti.start_date:
        # Calculate duration if not populated yet
        duration = (datetime.utcnow() - ti.start_date).total_seconds()
    
    status = ti.state if ti else "failed"

    # Construct the alert message
    alert_payload = {
        "task_name": task_id,  # Using task_id as requested
        "task_id": task_id,
        "dag_id": dag_id,
        "owner": owner,
        "task_start_time": start_time,
        "task_end_time": end_time,
        "task_batch_runId": run_id,
        "duration": duration,
        "task_status": status,
    }

    # Log the alert (this can be picked up by Cloud Logging log-based alerts)
    logging.error(f"AIRFLOW_ALERT_PAYLOAD: {json.dumps(alert_payload, indent=2)}")
    
    # Here you would add code to send to external systems, e.g.:
    # requests.post("https://your-webhook-url", json=alert_payload)

default_args = {
    'owner': 'demo_user',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # We can set the callback at the default_args level to apply to all tasks in the DAG
    'on_failure_callback': custom_failure_callback,
}

with DAG(
    'airflow_alert_demo_dag',
    default_args=default_args,
    description='A demo DAG showing how to send alerts with rich metadata',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    def fail_task():
        raise Exception("Simulated task failure for demo purposes!")

    run_this_to_fail = PythonOperator(
        task_id='simulated_failure_task',
        python_callable=fail_task,
        # Or you can set it at the task level:
        # on_failure_callback=custom_failure_callback,
    )

    def success_task():
        print("Task succeeded!")

    run_this_to_succeed = PythonOperator(
        task_id='success_task',
        python_callable=success_task,
    )

    run_this_to_succeed >> run_this_to_fail
