import argparse
from google.cloud import dataplex_v1

def list_dataplex_entities(project_id, location, lake_id, zone_id):
    """Lists entities in a Dataplex zone."""
    client = dataplex_v1.DataplexServiceClient()
    parent = f"projects/{project_id}/locations/{location}/lakes/{lake_id}/zones/{zone_id}"
    request = dataplex_v1.ListEntitiesRequest(parent=parent)
    page_result = client.list_entities(request=request)
    
    print("Entities:")
    for entity in page_result:
        print(f"- {entity.name}")
        print(f"  Display Name: {entity.display_name}")
        print(f"  Data Path: {entity.data_path}")
        print(f"  Type: {entity.type_}")
        print(f"  Asset: {entity.asset}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="List entities in a Dataplex zone."
    )
    parser.add_argument("project_id", help="Google Cloud project ID.")
    parser.add_argument("location", help="The region of the Dataplex instance.")
    parser.add_argument("lake_id", help="The ID of the Dataplex lake.")
    parser.add_argument("zone_id", help="The ID of the Dataplex zone.")
    args = parser.parse_args()

    list_dataplex_entities(args.project_id, args.location, args.lake_id, args.zone_id)
