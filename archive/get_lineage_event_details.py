import argparse
from google.cloud.datacatalog_lineage_v1 import LineageClient

def get_lineage_event_details(event_name):
    """
    Gets the details for a specific lineage event.

    Args:
        event_name: The full resource name of the lineage event.
    """
    client = LineageClient()

    try:
        event = client.get_lineage_event(name=event_name)
        print("Successfully retrieved lineage event:")
        print(event)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get the details of a Data Catalog lineage event."
    )
    parser.add_argument("event_name", help="The name of the lineage event.")
    args = parser.parse_args()

    get_lineage_event_details(args.event_name)
