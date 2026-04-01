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
