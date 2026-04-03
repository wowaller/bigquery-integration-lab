import argparse
import os
from google.cloud import bigquery

def monitor_universal_pipelines(project_id, hours=24, failed_only=False, region="us", key_path=None):
    """
    Scans BigQuery Job History for ALL jobs labeled as Pipelines or Dataform runs.
    """
    if key_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    client = bigquery.Client(project=project_id)

    # Base query to find jobs with Dataform/Pipeline labels
    query = f"""
    SELECT 
      job_id,
      user_email,
      start_time,
      end_time,
      TIMESTAMP_DIFF(end_time, start_time, SECOND) as duration_seconds,
      state,
      error_result.message as error_msg,
      query,
      
      -- Extracting Labels dynamically
      COALESCE(
        (SELECT value FROM UNNEST(labels) WHERE key = 'dag_name'),
        (SELECT value FROM UNNEST(labels) WHERE key = 'dataform_repository_id'),
        'Unknown_DAG'
      ) AS dag_name,
      
      COALESCE(
        (SELECT value FROM UNNEST(labels) WHERE key = 'task_name'),
        (SELECT value FROM UNNEST(labels) WHERE key = 'dataform_workflow_execution_action_id_name'),
        'Unknown_Task'
      ) AS task_name,
      
      COALESCE(
        (SELECT value FROM UNNEST(labels) WHERE key = 'run_id'),
        (SELECT value FROM UNNEST(labels) WHERE key = 'dataform_workflow_execution_id'),
        'Unknown_Run'
      ) AS run_id
    FROM 
      `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE 
      creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
      -- Find jobs that belong to Dataform or visual pipelines
      AND (
        EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key LIKE 'dataform_%')
        OR EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'dag_name')
      )
    """

    if failed_only:
        query += " AND error_result.message IS NOT NULL"

    query += " ORDER BY start_time DESC LIMIT 500"

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hours", "INT64", hours),
        ]
    )

    print(f"🔍 Scanning for Pipeline activity in region: {region} (Last {hours} hours)")
    if failed_only:
        print("🚨 Filtering for FAILURES ONLY")
    print("-" * 60)

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        count = 0
        for row in results:
            count += 1
            print(f"📊 DAG / Pipeline: {row.dag_name}")
            print(f"   Task Name:      {row.task_name}")
            print(f"   Job ID:         {row.job_id}")
            print(f"   Run ID:         {row.run_id}")
            print(f"   Owner:          {row.user_email}")
            print(f"   Start Time:     {row.start_time}")
            print(f"   End Time:       {row.end_time}")
            print(f"   Duration:       {row.duration_seconds} seconds")
            
            status = row.state
            if row.error_msg:
                status = "FAILED"
                print(f"   Status:         {status}")
                print(f"   Error:          {row.error_msg}")
            else:
                print(f"   Status:         {status}")
            print("-" * 60)

        if count == 0:
            print("ℹ️ No pipeline activity found in this time window.")

    except Exception as e:
        print(f"❌ Error querying BigQuery Job History: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal Pipeline and Dataform Monitor.")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--hours", type=int, default=24, help="Time window in hours (default: 24)")
    parser.add_argument("--failed-only", action="store_true", help="Filter for failed jobs only")
    parser.add_argument("--region", default="us", help="BigQuery region (default: us)")
    parser.add_argument("--key", help="Optional path to Service Account JSON key file")

    args = parser.parse_args()

    # Pass failed_only as boolean
    monitor_universal_pipelines(args.project, args.hours, args.failed_only, args.region, args.key)
