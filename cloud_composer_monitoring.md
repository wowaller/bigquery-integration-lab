# Part 3: Monitoring Cloud Composer Jobs

This document explains how to monitor Cloud Composer (Airflow) jobs using the Airflow REST API, with a focus on detecting and reporting failures.

## Direct Database Access: Not Recommended

Directly accessing the Airflow database in a Cloud Composer environment is not a supported or recommended practice. Cloud Composer is a managed service, and such access can be unstable and may break with future updates. The correct way to interact with Airflow programmatically is through its REST API.

## Authenticating with the Airflow REST API in Cloud Composer

Cloud Composer environments are secured with Identity-Aware Proxy (IAP). To access the Airflow REST API, you need to obtain an IAP-authenticated JWT token and include it in your requests.

## Prerequisites

1.  **Python and Pip:** Python (3.6+) and pip are required.
2.  **Google Cloud SDK:** Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) and authenticate:
    ```bash
    gcloud auth login
    gcloud auth application-default login
    ```
3.  **Enable necessary APIs:** Ensure the Cloud Composer API is enabled in your project.
4.  **Permissions:** The user or service account running the script needs the `roles/composer.user` and `roles/iap.securedAppUser` IAM roles.

## Installation

This script requires the `requests` and `google-auth` libraries:

```bash
pip install requests google-auth
```

## Python Script

Save the following script as `monitor_composer_jobs.py`:

```python
import argparse
import google.auth
from google.auth.transport.requests import AuthorizedSession
import datetime
import requests
import json
import urllib.parse

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

def monitor_dag(composer_url, dag_id="~", hours=24, states=None):
    """Monitors Cloud Composer jobs with caching, time window, and state filtering."""
    if not composer_url.endswith('/'):
        composer_url += '/'
    
    # Use the wildcard endpoint to fetch task instances directly
    api_url = f"{composer_url}api/v1/dags/{dag_id}/dagRuns/~/taskInstances"

    authed_session = AuthorizedSession(CREDENTIALS)
    dag_owner_cache = {}

    # Calculate time window filter (wall-clock start time)
    params = {"limit": 50}
    if hours:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        start_date_gte = now_utc - datetime.timedelta(hours=hours)
        params["start_date_gte"] = start_date_gte.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Limit by state in URL parameter (efficient push-down)
    if states:
        params["state"] = states

    # Fetch DAG owner if we are querying a single DAG (no cache needed)
    dag_owner = "N/A"
    if dag_id != "~":
        dag_details_url = f"{composer_url}api/v1/dags/{dag_id}"
        try:
            dag_response = authed_session.get(dag_details_url)
            dag_response.raise_for_status()
            dag_details = dag_response.json()
            dag_owner = ", ".join(dag_details.get("owners", [])) or "N/A"
            dag_owner_cache[dag_id] = dag_owner
        except Exception as e:
            print(f"Warning: Could not fetch DAG owner: {e}")

    try:
        response = authed_session.get(api_url, params=params)
        response.raise_for_status()
        task_instances = response.json().get("task_instances", [])

        for task in task_instances:
            task_dag_id = task["dag_id"]
            
            # Resolve owner using cache for All-DAGs mode
            current_owner = "N/A"
            if dag_id != "~":
                 current_owner = dag_owner
            else:
                 if task_dag_id in dag_owner_cache:
                      current_owner = dag_owner_cache[task_dag_id]
                 else:
                      dag_details_url = f"{composer_url}api/v1/dags/{task_dag_id}"
                      try:
                          dag_response = authed_session.get(dag_details_url)
                          dag_response.raise_for_status()
                          dag_details = dag_response.json()
                          resolved_owner = ", ".join(dag_details.get("owners", [])) or "N/A"
                          dag_owner_cache[task_dag_id] = resolved_owner
                          current_owner = resolved_owner
                      except Exception as e:
                          print(f"Warning: Could not fetch DAG owner for {task_dag_id}: {e}")
                          dag_owner_cache[task_dag_id] = "N/A"
                          current_owner = "N/A"

            print(json.dumps({
                "task_name": task["task_id"],
                "task_id": task["task_id"],
                "dag_id": task_dag_id,
                "owner": current_owner,
                "task_start_time": task["start_date"],
                "task_end_time": task["end_date"],
                "task_batch_runId": task.get("dag_run_id", "N/A"),
                "duration": task["duration"],
                "task_status": task["state"],
            }, indent=4))

    except requests.exceptions.RequestException as e:
        print(f"Error accessing Airflow API: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor Cloud Composer jobs.")
    parser.add_argument("composer_url", help="URL of the Cloud Composer environment.")
    parser.add_argument("--dag_id", default="~", help="The ID of the DAG to monitor (default: '~' for all DAGs).")
    parser.add_argument("--hours", type=int, default=24, help="Search time window in hours (default: 24). Set to 0 for no time filter.")
    parser.add_argument("--states", nargs='+', help="Task states to filter by (e.g. failed success upstream_failed).")
    args = parser.parse_args()

    hours_filter = args.hours if args.hours > 0 else None
    
    monitor_dag(args.composer_url, args.dag_id, hours_filter, args.states)
```

## Airflow REST API Interfaces Used

This demo uses the Apache Airflow REST API to efficiently query task metadata. By pushing filters (like time windows and states) to the server, it reduces payload size and network latency.

### 1. Unified Task Instances Endpoint (The Double Wildcard)
*   **Endpoint:** `GET /api/v1/dags/{dag_id}/dagRuns/~/taskInstances`
*   **Purpose:** Retrieves a flat list of all tasks for a specific DAG or for **all DAGs** (if `dag_id` is set to the wildcard `~`).
*   **Why use it?** Typical implementations loop over DAG runs and then query tasks for each run. That incurs $O(N)$ network calls. This endpoint is a single $O(1)$ request that pulls all tasks across all runs in one go.
*   **Key URL Parameters Used:**
    *   `limit`: Prevents hanging by capping the result set.
    *   `start_date_gte`: Wall-clock time push-down filter. Filters tasks by their actual start time over UTC.
    *   `state`: URL query push-down filter for states (e.g., pulling only failures vs all states).

### 2. DAG Details Endpoint
*   **Endpoint:** `GET /api/v1/dags/{dag_id}`
*   **Purpose:** Retrieves metadata about a specific DAG (specifically `owners`).
*   **Why use it?** The Task Instance endpoint does not return the owner of the DAG context, so this endpoint resolves it.
*   **Optimization:** The script uses an in-memory **cache** (`dag_owner_cache`) to avoid making this call repeatedly for duplicate occurrences of the same DAG in the task list.

### 3. Google Authenticated Session
*   **Interface:** `google.auth.transport.requests.AuthorizedSession`
*   **Purpose:** Automatically attaches Google OAuth credentials (`IAM authentication`) to standard `requests` HTTP calls to satisfy Cloud Composer security requirements.

---

## How to Run the Script

1.  **Find your Composer URL:**
    *   Navigate to the Cloud Composer environment in the Google Cloud Console.
    *   The **Airflow UI URL** is your `composer_url`.

2.  **Run the script:**

    ```bash
    python monitor_composer_jobs.py YOUR_COMPOSER_URL YOUR_DAG_ID
    ```

## Testing

To test this, I will need:

*   The **URL of your Cloud Composer environment's Airflow UI**.
*   A **DAG ID** to monitor.

With this information, I can help you execute the script and analyze the results. (Note: The environment should be authenticated via `gcloud` or a service account).

## Real-time Alerting with Native Callbacks

The standard and most robust way to send alerts for task failures is to use Airflow's native `on_failure_callback`. This runs inside the Airflow environment and has access to rich metadata via the `context` object.

### Demo DAG: `airflow_alert_demo.py`

Create a file named `airflow_alert_demo.py` and upload it to your Composer environment's `dags/` folder.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging

def custom_failure_callback(context):
    ti = context.get('task_instance')
    dag = context.get('dag')
    
    # Extract fields
    task_id = ti.task_id if ti else "N/A"
    dag_id = ti.dag_id if ti else "N/A"
    owner = ti.owner if ti else "N/A"
    start_time = ti.start_date.isoformat() if ti and ti.start_date else "N/A"
    end_time = ti.end_date.isoformat() if ti and ti.end_date else datetime.utcnow().isoformat()
    run_id = context.get('run_id', "N/A")
    
    duration = ti.duration if ti and ti.duration else None
    if duration is None and ti and ti.start_date:
        duration = (datetime.utcnow() - ti.start_date).total_seconds()
    
    status = ti.state if ti else "failed"

    alert_payload = {
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

    # Log the alert (picked up by Cloud Logging)
    logging.error(f"AIRFLOW_ALERT_PAYLOAD: {json.dumps(alert_payload, indent=2)}")
    
    # Send to external systems (uncomment and configure)
    # import requests
    # requests.post("https://your-webhook-url", json=alert_payload)

default_args = {
    'owner': 'demo_user',
    'start_date': datetime(2023, 1, 1),
    'on_failure_callback': custom_failure_callback, # Set globally for the DAG
}

with DAG(
    'airflow_alert_demo_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    run_this_to_fail = PythonOperator(
        task_id='simulated_failure_task',
        python_callable=lambda: (_ for _ in ()).throw(Exception("Simulated failure!")),
    )
```

---

## Infrastructure-Level Alerting with GCP Console

You can also set up alerts using Google Cloud Logging and Cloud Monitoring without modifying your DAG code (though standardized logging as shown above makes it easier).

### Approach 1: Log-Based Alerting (Recommended)

1.  **Navigate to Logs Explorer:** In the Google Cloud Console, go to **Logging > Logs Explorer**.
2.  **Define a Filter:**
    *   To catch general failures: `resource.type="cloud_composer_environment" AND textPayload:"Task failed"`
    *   To catch specific payloads (like the one from `airflow_alert_demo.py`): `resource.type="cloud_composer_environment" AND textPayload:"AIRFLOW_ALERT_PAYLOAD"`
3.  **Create Alert:** Click **Create Alert** in the toolbar.
    *   Set a name (e.g., "Airflow Task Failure").
    *   Define the trigger (e.g., if the log appears 1 time).
    *   Select a **Notification Channel** (Email, Slack, SMS, Pub/Sub).

### Approach 2: Log-Based Metrics

If you want to track failure counts or set thresholds over time:

1.  Go to **Logging > Log-based Metrics** and create a **Counter** metric with your filter.
2.  Go to **Monitoring > Alerting** and create a policy based on your new metric.

---

## Further Reading

For a detailed reference of the Airflow REST API, please see the official documentation:

*   [Airflow REST API Reference](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
