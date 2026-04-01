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
        # In python client, attributes might act like a dict, let's try direct lookup first or loop
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
