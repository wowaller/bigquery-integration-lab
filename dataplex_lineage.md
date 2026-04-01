# Part 2: Exporting Lineage Data from Data Catalog

This document explains how to export lineage data from Google Cloud Data Catalog using the Data Lineage API and a Python script.

## Background: Data Catalog Lineage API

Lineage information is accessed programmatically through the Data Lineage API. We will use the `google-cloud-datacatalog-lineage` Python library to interact with this API.

## Prerequisites

1.  **Python and Pip:** Ensure you have Python (3.6+) and pip installed.
2.  **Google Cloud SDK:** Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) and authenticate:
    ```bash
    gcloud auth login
    gcloud auth application-default login
    ```
3.  **Enable the Data Catalog API:** Make sure the Data Catalog API is enabled for your project in the Google Cloud Console.

## Installation

Install the required Python libraries:

```bash
pip install google-cloud-datacatalog-lineage google-cloud-bigquery
```

## Python Script

Here is the Python script to export lineage data. Save it as `get_lineage.py`.

```python
import argparse
from google.cloud.datacatalog_lineage_v1 import LineageClient, EntityReference
from google.cloud import bigquery
from google.protobuf.json_format import MessageToJson

def get_bigquery_job_details(project_id, job_id, location):
    """
    Gets the details of a BigQuery job.

    Args:
        project_id: The Google Cloud project ID.
        job_id: The BigQuery job ID.
        location: The location of the BigQuery job.
    """
    client = bigquery.Client(project=project_id)
    try:
        job = client.get_job(job_id, location=location)
        print(f"    BigQuery Job Details ({job_id}):")
        print(f"      User: {job.user_email}")
        print(f"      Query: {job.query}")
    except Exception as e:
        print(f"    Could not get BigQuery job details for {job_id}: {e}")

def get_processes_from_links(project_id, location, links):
    """
    Gets the processes associated with a list of lineage links.

    Args:
        project_id: The Google Cloud project ID.
        location: The region of the Data Catalog instance.
        links: A list of lineage link names.
    """
    lineage_client = LineageClient()

    request = {
        "parent": f"projects/{project_id}/locations/{location}",
        "links": links,
    }

    try:
        response = lineage_client.batch_search_link_processes(request=request)
        for process_links in response:
            print(f"Details for link: {process_links.links[0].link}")
            process_name = process_links.process
            process = lineage_client.get_process(name=process_name)
            print("  Process Details (RAW):")
            print(MessageToJson(process._pb))


    except Exception as e:
        print(f"An error occurred: {e}")

def get_column_lineage(project_id, location, dataset_id, table_id, column):
    """
    Gets the column lineage for a specific BigQuery column.

    Args:
        project_id: The Google Cloud project ID.
        location: The region of the Data Catalog instance.
        dataset_id: The BigQuery dataset ID.
        table_id: The BigQuery table ID.
        column: The name of the column.
    """
    lineage_client = LineageClient()
    
    target = f"bigquery:{project_id}.{dataset_id}.{table_id}"
    
    request = {
        "parent": f"projects/{project_id}/locations/{location}",
        "target": EntityReference(
            fully_qualified_name=target
        ),
    }

    try:
        print(f"Searching for column lineage links with target: {target}")
        results = lineage_client.search_links(request=request)
        for link in results:
            print("Found column lineage link:")
            print(f"  Name: {link.name}")
            print(f"  Source: {link.source.fully_qualified_name}")
            print(f"  Target: {link.target.fully_qualified_name}")
            print(f"  Start Time: {link.start_time}")
            print(f"  End Time: {link.end_time}")

    except Exception as e:
        print(f"An error occurred while getting column lineage: {e}")

def run_lineage_demo(project_id, location, dataset_id, table_id):
    """ Runs the full demo flow: Table -> Links -> Process -> Job Details """
    lineage_client = LineageClient()
    
    target = f"bigquery:{project_id}.{dataset_id}.{table_id}"
    
    print(f"\n1. Searching for table lineage links with target: {target}")
    request = {
        "parent": f"projects/{project_id}/locations/{location}",
        "target": EntityReference(fully_qualified_name=target),
    }
    links_response = lineage_client.search_links(request=request)
    
    links = [link.name for link in links_response]
    if not links:
        print("No lineage links found.")
        return

    print(f"Found {len(links)} links. Fetching processes...")

    process_request = {
        "parent": f"projects/{project_id}/locations/{location}",
        "links": links,
    }
    process_links_list = lineage_client.batch_search_link_processes(request=process_request)

    for process_link in process_links_list:
        print(f"\nDetails for link: {process_link.links[0].link}")
        process_name = process_link.process
        process = lineage_client.get_process(name=process_name)
        
        print(f"  Process Name: {process.name}")
        
        job_id = None
        if "bigquery_job_id" in process.attributes:
             job_id = process.attributes["bigquery_job_id"]
        
        if not job_id:
             for key, value in process.attributes.items():
                 if key == "bigquery_job_id":
                     job_id = value
                     break
                     
        if job_id:
            job_id_str = str(job_id)
            print(f"  Found Job ID: {job_id_str}")
            get_bigquery_job_details(project_id, job_id_str, location)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get the processes and lineage information from Data Catalog."
    )
    parser.add_argument("project_id", help="Google Cloud project ID.")
    parser.add_argument("location", help="The region of the Data Catalog instance.")
    parser.add_argument("--links", nargs="+", help="The names of the lineage links.")
    parser.add_argument("--dataset_id", help="The BigQuery dataset ID for column lineage.")
    parser.add_argument("--table_id", help="The BigQuery table ID for column lineage.")
    parser.add_argument("--column", help="The column name for column lineage.")
    args = parser.parse_args()

    if args.links:
        get_processes_from_links(args.project_id, args.location, args.links)
    
    if args.dataset_id and args.table_id:
        if args.column:
            get_column_lineage(args.project_id, args.location, args.dataset_id, args.table_id, args.column)
        else:
            run_lineage_demo(args.project_id, args.location, args.dataset_id, args.table_id)
```

```

## How to Run the Script

The script can be used to get table lineage, process details, and column lineage.

### Get Table Lineage and Process Details

To get the process details for a list of lineage links, you need to provide your project ID, location, and the names of the lineage links.

The lineage link names can be found using the `get_lineage.py` script from the previous steps.

**Example command:**

```bash
python get_lineage.py YOUR_PROJECT_ID YOUR_LOCATION --links "projects/YOUR_PROJECT_ID/locations/YOUR_LOCATION/links/LINK_ID_1" "projects/YOUR_PROJECT_ID/locations/YOUR_LOCATION/links/LINK_ID_2"
```

Replace `YOUR_PROJECT_ID`, `YOUR_LOCATION`, and `LINK_ID_1`, `LINK_ID_2` with your actual values.

### Get Column Lineage

To get the column lineage for a specific BigQuery column, you need to provide your project ID, location, dataset ID, table ID, and column name.

**Example command:**

```bash
python get_lineage.py YOUR_PROJECT_ID YOUR_LOCATION --dataset_id YOUR_DATASET --table_id YOUR_TABLE --column YOUR_COLUMN
```

Replace `YOUR_PROJECT_ID`, `YOUR_LOCATION`, `YOUR_DATASET`, `YOUR_TABLE`, and `YOUR_COLUMN` with your actual values.

### Run Unified Lineage Demo (Table -> Links -> Process -> BQ Job)

To run the full demo that searches for table links, fetches their processes, and pulls the BigQuery job details:

```bash
python get_lineage.py YOUR_PROJECT_ID YOUR_LOCATION --dataset_id YOUR_DATASET --table_id YOUR_TABLE
```

This unified flow will print the SQL query history for the table creation!


## Understanding the Data Lineage API Interfaces

The enriched `get_lineage.py` script leverages the following Data Lineage API client interfaces (`v1` client library):

*   **`LineageClient`**: The primary client used to search links, get processes, and fetch lineage data.
*   **`SearchLinks` / `search_links(...)`**: Finds lineage links where a specific asset (e.g., a BigQuery table represented by FQN `bigquery:project.dataset.table`) is the target.
*   **`BatchSearchLinkProcesses` / `batch_search_link_processes(...)`**: Resolves the processes associated with a group of links. This is used to find which job generated the lineage.
*   **`GetProcess` / `get_process(...)`**: Retrieves the full details of a specific process, such as its name, display name, attributes mapping (including `bigquery_job_id`), and origin.

---

## Authentication and Setup for Your Application

To use this script and build your own applications using the Data Lineage API, you must authenticate correctly and set up environment variables.

### 1. Local Testing (User Credentials)

Run the following commands to authenticate using your Google user credentials and create Application Default Credentials (ADC):

```bash
gcloud auth login
gcloud auth application-default login
```

### 2. Production Testing or Automated Runs (Service Account)

If you are running this from a server or automated script, create a **Service Account**, download its JSON key file, and specify its path in the environment:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

#### Required IAM Permissions:
Make sure your user or service account has the following IAM roles in your project:
*   `roles/datalineage.viewer` (to query lineage links and processes)
*   `roles/bigquery.jobUser` (to see the SQL query history for jobs)
*   `roles/bigquery.dataViewer` (to run queries and view schema metadata)

---

## Reference URIs

*   [Data Catalog Lineage API Concepts](https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage)
*   [Python API Reference: Data Catalog Lineage Client](https://cloud.google.com/python/docs/reference/datalineage/latest)
*   [BigQuery Job Methods Documentation](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs)
*   [Google API Core Authentication](https://googleapis.dev/python/google-api-core/latest/auth.html)

---

## Further Reading

For more detailed information on the Google Cloud Data Catalog Python client library, see the official documentation:

*   [google-cloud-datacatalog-lineage Python Client Library Documentation](https://cloud.google.com/python/docs/reference/datalineage/latest)
*   [google-cloud-bigquery Python Client Library Documentation](https://cloud.google.com/python/docs/reference/bigquery/latest)

```
