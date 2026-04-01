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
        print(f"  Location: {dataset.location}")
        tables_to_process = []
        if table_id:
            try:
                table = client.get_table(f"{project_id}.{dataset.dataset_id}.{table_id}")
                tables_to_process.append(table)
            except Exception as e:
                print(f"    Could not get table {table_id} in dataset {dataset.dataset_id}: {e}")
                continue

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
