import argparse
from google.cloud.datacatalog_lineage_v1 import LineageClient, EntityReference

def search_table_lineage(project_id, location, dataset_id, table_id):
    """Searches for lineage links where the table is the target."""
    lineage_client = LineageClient()
    
    target = f"projects/{project_id}/locations/{location}/entryGroups/@bigquery/entries/{dataset_id}.{table_id}"

    request = {
        "parent": f"projects/{project_id}/locations/{location}",
        "target": EntityReference(
            fully_qualified_name=target
        ),
    }

    try:
        print(f"Searching for table lineage links with target: {target}")
        results = lineage_client.search_links(request=request)
        found = False
        for link in results:
            found = True
            print("Found table lineage link:")
            print(f"  Name: {link.name}")
            print(f"  Source: {link.source.fully_qualified_name}")
            print(f"  Target: {link.target.fully_qualified_name}")
            print(f"  Start Time: {link.start_time}")
            print(f"  End Time: {link.end_time}")
        if not found:
            print("No links found.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search table lineage.")
    parser.add_argument("project_id")
    parser.add_argument("location")
    parser.add_argument("dataset_id")
    parser.add_argument("table_id")
    args = parser.parse_args()

    search_table_lineage(args.project_id, args.location, args.dataset_id, args.table_id)
