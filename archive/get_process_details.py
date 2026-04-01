import argparse
from google.cloud.datacatalog_lineage_v1 import LineageClient

def get_process_details(process_name):
    """
    Gets the details of a specific process from Data Catalog.

    Args:
        process_name: The name of the process to get details for.
    """
    client = LineageClient()

    try:
        process = client.get_process(name=process_name)
        print("Process Details:")
        print(process)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get the details of a Data Catalog process."
    )
    parser.add_argument("process_name", help="The name of the process.")
    args = parser.parse_args()

    get_process_details(args.process_name)
