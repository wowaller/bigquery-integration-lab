# Part 1: Collecting BigQuery Schema Information

This document provides a Python script to collect detailed schema information from BigQuery.

## Prerequisites

1.  **Python and Pip:** Make sure you have Python (3.6+) and pip installed.
2.  **Google Cloud SDK:** Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) and authenticate with your Google Cloud account:
    ```bash
    gcloud auth login
    gcloud auth application-default login
    ```
3.  **Enable the BigQuery API:** Ensure the BigQuery API is enabled for your project. You can do this from the Google Cloud Console.

## Installation

You need to install the `google-cloud-bigquery` Python library:

```bash
pip install google-cloud-bigquery
```

## Python Script

Here is the Python script to collect schema information. Save it as `get_bigquery_schema.py`.

```python
import argparse
from google.cloud import bigquery

def get_schema_info(project_id, dataset_id=None, table_id=None):
    """
    Prints schema information for BigQuery tables.

    Args:
        project_id: The Google Cloud project ID.
        dataset_id: The BigQuery dataset ID (optional).
        table_id: The BigQuery table ID (optional).
    """
    client = bigquery.Client(project=project_id)

    datasets = []
    if dataset_id:
        datasets.append(client.get_dataset(dataset_id))
    else:
        datasets = list(client.list_datasets())

    print(f"Project: {project_id}")
    for dataset in datasets:
        print(f"  Dataset: {dataset.dataset_id}")
        tables_to_process = []
        if table_id:
            tables_to_process.append(client.get_table(f"{project_id}.{dataset.dataset_id}.{table_id}"))
        else:
            tables_to_process = list(client.list_tables(dataset.dataset_id))

        for table_item in tables_to_process:
            table_ref = dataset.table(table_item.table_id)
            table = client.get_table(table_ref)
            print(f"    Table: {table.table_id}")
            print(f"      Description: {table.description}")
            print("      Columns:")
            for field in table.schema:
                print(f"        - Name: {field.name}")
                print(f"          Type: {field.field_type}")
                print(f"          Mode: {field.mode}")
                print(f"          Description: {field.description}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get BigQuery schema information."
    )
    parser.add_argument("project_id", help="Google Cloud project ID.")
    parser.add_argument("-d", "--dataset_id", help="BigQuery dataset ID (optional).")
    parser.add_argument("-t", "--table_id", help="BigQuery table ID (optional).")
    args = parser.parse_args()

    get_schema_info(args.project_id, args.dataset_id, args.table_id)
```

## How to Run the Script

You can run the script from your terminal with different levels of specificity:

**1. Get schema for all tables in a project:**

```bash
python get_bigquery_schema.py YOUR_PROJECT_ID
```

**2. Get schema for all tables in a specific dataset:**

```bash
python get_bigquery_schema.py YOUR_PROJECT_ID -d YOUR_DATASET_ID
```

**3. Get schema for a specific table:**

```bash
python get_bigquery_schema.py YOUR_PROJECT_ID -d YOUR_DATASET_ID -t YOUR_TABLE_ID
```

Replace `YOUR_PROJECT_ID`, `YOUR_DATASET_ID`, and `YOUR_TABLE_ID` with your actual values.

## Testing

To test this, please provide me with:

*   Your Google Cloud **Project ID**.
*   Optionally, a **Dataset ID** and/or a **Table ID** if you want to test with a specific scope.

I can then help you run the command and verify the output.

## Further Reading

For more detailed information on the Google Cloud BigQuery Python client library, see the official documentation:

*   [google-cloud-bigquery Python Client Library Documentation](https://cloud.google.com/python/docs/reference/bigquery/latest)
