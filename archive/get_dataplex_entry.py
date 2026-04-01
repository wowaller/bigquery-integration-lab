import argparse
import base64
from google.cloud import dataplex_v1

def get_dataplex_entry(project_id, location, dataset_id, table_id):
    """
    Gets a specific entry from a Dataplex entry group.

    Args:
        project_id: The ID of the Google Cloud project.
        location: The location of the Dataplex resources.
        dataset_id: The BigQuery dataset ID.
        table_id: The BigQuery table ID.
    """
    client = dataplex_v1.DataplexServiceClient()

    fqn = f"bigquery:{project_id}.{dataset_id}.{table_id}"
    entry_id = base64.b64encode(fqn.encode("utf-8")).decode("utf-8")
    entry_group_id = "@bigquery"

    name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group_id}/entries/{entry_id}"

    try:
        entry = client.get_entry(name=name)
        print("Successfully retrieved entry:")
        print(entry)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get a Dataplex entry for a BigQuery table."
    )
    parser.add_argument("project_id", help="Google Cloud project ID.")
    parser.add_argument("location", help="The region of the Dataplex instance.")
    parser.add_argument("dataset_id", help="The BigQuery dataset ID.")
    parser.add_argument("table_id", help="The BigQuery table ID.")
    args = parser.parse_args()

    get_dataplex_entry(args.project_id, args.location, args.dataset_id, args.table_id)
