import argparse
from google.cloud import dataplex_v1

def get_lineage(project_id, location, entity_name):
    """
    Exports lineage data for a specific entity from Dataplex.

    Args:
        project_id: The Google Cloud project ID.
        location: The region of the Dataplex instance.
        entity_name: The name of the entity to trace the lineage of.
    """
    client = dataplex_v1.LineageClient()

    request = dataplex_v1.SearchLinksRequest(
        parent=f"projects/{project_id}/locations/{location}",
        source={'fully_qualified_name': entity_name},
    )

    try:
        response = client.search_links(request=request)
        print(f"Lineage for: {entity_name}")
        for link in response.links:
            print(f"  - Source: {link.source.fully_qualified_name}")
            print(f"    Target: {link.target.fully_qualified_name}")
            print(f"    Start Time: {link.start_time}")
            print(f"    End Time: {link.end_time}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export lineage data from Dataplex."
    )
    parser.add_argument("project_id", help="Google Cloud project ID.")
    parser.add_argument("location", help="The region of the Dataplex instance.")
    parser.add_argument("entity_name", help="The name of the entity to trace.")
    args = parser.parse_args()

    get_lineage(args.project_id, args.location, args.entity_name)
