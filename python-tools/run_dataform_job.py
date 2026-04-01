import os
import pendulum
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# GCP Constants
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "binggang-lab")
REGION = "us-east4"
REPOSITORY_ID = "c029ce7a-38e6-471a-863a-936349174d93"
WORKSPACE_ID = "default"
# TODO: Replace with your actual Service Account email. This service account will execute the Dataform job.
# Example: "your-service-account@your-project-id.iam.gserviceaccount.com"
SERVICE_ACCOUNT = "composer@binggang-lab.iam.gserviceaccount.com"

import requests
from datetime import datetime

def failure_webhook_callback(context):
    """
    Airflow failure callback that sends a POST JSON payload to a webhook.
    Formed with the exact same fields as the Composer Monitor code.
    """
    ti = context.get('task_instance')
    dag = context.get('dag')
    
    # Extract metadata context variables
    task_id = ti.task_id if ti else "N/A"
    dag_id = ti.dag_id if ti else "N/A"
    owner = ti.owner if ti else "N/A"
    
    # Airflow Timestamps
    start_time = ti.start_date.isoformat() if ti and ti.start_date else "N/A"
    
    # For end time on failure, we take the current time of the failure event
    end_time = datetime.utcnow().isoformat() + "Z" 
    
    run_id = context.get('run_id', "N/A")
    
    # Calculate duration
    duration = ti.duration if ti and ti.duration else None
    if duration is None and ti and ti.start_date:
         duration = (datetime.utcnow() - ti.start_date).total_seconds()

    status = ti.state if ti else "failed"

    payload = {
        "task_name": task_id,
        "task_id": task_id,
        "dag_id": dag_id,
        "owner": owner,
        "task_start_time": start_time,
        "task_end_time": end_time,
        "task_batch_runId": run_id,
        "duration": duration,
        "task_status": status,
    }

    # Dummy Receiver (httpbin)
    WEBHOOK_URL = "https://httpbin.org/post"

    try:
        response = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        print(f"Alert sent successfully. Webhook Status: {response.status_code}")
    except Exception as e:
        print(f"Failed to send webhook alert: {e}")

DAG_ID = "dataform_pipeline_test"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dataform", "gcp"],
    on_failure_callback=failure_webhook_callback,
) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
             "workspace": (
                 f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                 f"workspaces/{WORKSPACE_ID}"
             ),
             # Example of passing arguments (vars) during compilation:
             # "code_compilation_config": {
             #    "vars": {
             #        "execution_date": "{{ ds }}", # Airflow macro example
             #        "my_custom_var": "value",
             #    }
             # }
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
                # This tells Dataform to run the SQL operations as this specific service account
                "service_account": SERVICE_ACCOUNT
            }
        },
    )

    create_compilation_result >> create_workflow_invocation
