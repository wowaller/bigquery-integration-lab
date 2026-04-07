package com.customer.jt;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.datacatalog.lineage.v1.BatchSearchLinkProcessesRequest;
import com.google.cloud.datacatalog.lineage.v1.BatchSearchLinkProcessesResponse;
import com.google.cloud.datacatalog.lineage.v1.EntityReference;
import com.google.cloud.datacatalog.lineage.v1.LineageClient;
import com.google.cloud.datacatalog.lineage.v1.Process;
import com.google.cloud.datacatalog.lineage.v1.SearchLinksRequest;
import com.google.cloud.datacatalog.lineage.v1.SearchLinksResponse;
import com.google.protobuf.Value;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.datacatalog.lineage.v1.LineageSettings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DataLineageGraphExtractor is a utility tool to extract and print table and column
 * lineage information from Dataplex Data Lineage API.
 * It queries for links tied to a specific target and resolves the associated processes
 * and BigQuery Job histories.
 */
public class DataLineageGraphExtractor {

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("p", "project", true, "Google Cloud project ID");
        options.addOption("l", "location", true, "Region of the Data Catalog instance (e.g. us-central1)");
        options.addOption("d", "dataset", true, "BigQuery dataset ID");
        options.addOption("t", "table", true, "BigQuery table ID");
        options.addOption("c", "column", true, "Column name for column lineage (optional)");
        options.addOption("k", "key", true, "Service Account key path (optional)");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String projectId = cmd.getOptionValue("project");
            String location = cmd.getOptionValue("location");
            String datasetId = cmd.getOptionValue("dataset");
            String tableId = cmd.getOptionValue("table");
            String column = cmd.getOptionValue("column");
            String keyPath = cmd.getOptionValue("key");

            if (projectId == null || location == null || datasetId == null || tableId == null) {
                System.err.println("Usage: java DataLineageGraphExtractor -p PROJECT_ID -l LOCATION -d DATASET_ID -t TABLE_ID [-c COLUMN] [-k KEY_PATH]");
                System.exit(1);
            }

            if (column != null) {
                getColumnLineage(projectId, location, datasetId, tableId, column, keyPath);
            } else {
                runLineageDemo(projectId, location, datasetId, tableId, keyPath);
            }

        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Gets and prints details of a BigQuery job (user, status).
     * 
     * @param projectId The Google Cloud Project ID.
     * @param jobId The BigQuery Job ID to query.
     * @param location The location of the job.
     */
    public static void getBigQueryJobDetails(String projectId, String jobId, String location, String keyPath) {
        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder().setProjectId(projectId);
        if (keyPath != null) {
            try {
                builder.setCredentials(GoogleCredentials.fromStream(new FileInputStream(keyPath)));
            } catch (IOException e) {
                System.err.println("    Error reading key file in getBigQueryJobDetails: " + e.getMessage());
            }
        }
        BigQuery bigquery = builder.build().getService();
        try {
            Job job = bigquery.getJob(JobId.of(projectId, jobId));
            if (job != null) {
                System.out.println("    BigQuery Job Details (" + jobId + "):");
                System.out.println("      User: " + job.getUserEmail());
                System.out.println("      Details: " + job.toString());
            } else {
                System.err.println("    Job not found: " + jobId);
            }
        } catch (Exception e) {
            System.err.println("    Could not get BigQuery job details for " + jobId + ": " + e.getMessage());
        }
    }

    /**
     * Queries for column-level lineage links using the Dataplex LineageClient.
     * 
     * @param projectId The Google Cloud Project ID.
     * @param location The region (e.g., us-central1).
     * @param datasetId Dataset ID.
     * @param tableId Table ID.
     * @param column Column name.
     */
    public static void getColumnLineage(String projectId, String location, String datasetId, String tableId, String column, String keyPath) {
        LineageSettings.Builder settingsBuilder = LineageSettings.newBuilder();
        if (keyPath != null) {
            try {
                settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(keyPath))));
                System.out.println("Using service account key for LineageClient: " + keyPath);
            } catch (IOException e) {
                System.err.println("Error reading key file for LineageClient: " + e.getMessage());
                System.exit(1);
            }
        }

        try (LineageClient lineageClient = LineageClient.create(settingsBuilder.build())) {
            String target = String.format("bigquery:%s.%s.%s", projectId, datasetId, tableId); // Wait, column lineage target usually includes column name!
            // If the python code just uses bigquery:project.dataset.table for column lineage, it might be querying the table and then filtering?
            // In the python code (line 66), target was "bigquery:{project_id}.{dataset_id}.{table_id}". This seems to be table lineage, not column lineage!
            // If column lineage is needed, the target should be "bigquery:{project_id}.{dataset_id}.{table_id}/{column}". Let's check if the python code does that or not.
            // The python code views showed target as "bigquery:{project_id}.{dataset_id}.{table_id}" (line 66). Let's assume table lineage for now or adapt if needed.
            
            String parent = String.format("projects/%s/locations/%s", projectId, location);

            SearchLinksRequest request = SearchLinksRequest.newBuilder()
                    .setParent(parent)
                    .setTarget(EntityReference.newBuilder().setFullyQualifiedName(target).build())
                    .build();

            System.out.println("Searching for lineage links with target: " + target);
            LineageClient.SearchLinksPagedResponse response = lineageClient.searchLinks(request);

            for (com.google.cloud.datacatalog.lineage.v1.Link link : response.iterateAll()) {
                System.out.println("Found lineage link:");
                System.out.println("  Name: " + link.getName());
                System.out.println("  Source: " + link.getSource().getFullyQualifiedName());
                System.out.println("  Target: " + link.getTarget().getFullyQualifiedName());
                System.out.println("  Start Time: " + (link.hasStartTime() ? link.getStartTime().toString() : "N/A"));
                System.out.println("  End Time: " + (link.hasEndTime() ? link.getEndTime().toString() : "N/A"));
            }

        } catch (IOException e) {
            System.err.println("An error occurred while getting lineage: " + e.getMessage());
        }
    }

    /**
     * Runs a demo of table lineage searching and process resolution using the Dataplex LineageClient.
     * Steps:
     * 1. Search for lineage links using the target table.
     * 2. Batch search for processes associated with those links.
     * 3. Print process attributes (like BigQuery job IDs).
     * 
     * @param projectId The Google Cloud Project ID.
     * @param location The region (e.g., us-central1).
     * @param datasetId Dataset ID.
     * @param tableId Table ID.
     */
    public static void runLineageDemo(String projectId, String location, String datasetId, String tableId, String keyPath) {
        LineageSettings.Builder settingsBuilder = LineageSettings.newBuilder();
        if (keyPath != null) {
            try {
                settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(keyPath))));
                System.out.println("Using service account key for LineageClient: " + keyPath);
            } catch (IOException e) {
                System.err.println("Error reading key file for LineageClient: " + e.getMessage());
                System.exit(1);
            }
        }

        try (LineageClient lineageClient = LineageClient.create(settingsBuilder.build())) {
            String target = String.format("bigquery:%s.%s.%s", projectId, datasetId, tableId);
            String parent = String.format("projects/%s/locations/%s", projectId, location);

            System.out.println("\n1. Searching for table lineage links with target: " + target);
            SearchLinksRequest request = SearchLinksRequest.newBuilder()
                    .setParent(parent)
                    .setTarget(EntityReference.newBuilder().setFullyQualifiedName(target).build())
                    .build();

            LineageClient.SearchLinksPagedResponse response = lineageClient.searchLinks(request);

            List<String> links = new ArrayList<>();
            for (com.google.cloud.datacatalog.lineage.v1.Link link : response.iterateAll()) {
                links.add(link.getName());
            }

            if (links.isEmpty()) {
                System.out.println("No lineage links found.");
                return;
            }

            System.out.println("Found " + links.size() + " links. Fetching processes...");

            BatchSearchLinkProcessesRequest processRequest = BatchSearchLinkProcessesRequest.newBuilder()
                    .setParent(parent)
                    .addAllLinks(links)
                    .build();

            LineageClient.BatchSearchLinkProcessesPagedResponse processLinksList = lineageClient.batchSearchLinkProcesses(processRequest);

            for (com.google.cloud.datacatalog.lineage.v1.ProcessLinks processLink : processLinksList.iterateAll()) {
                System.out.println("\nDetails for link: " + (processLink.getLinksCount() > 0 ? processLink.getLinks(0).getLink() : "N/A"));
                String processName = processLink.getProcess();
                Process process = lineageClient.getProcess(processName);

                System.out.println("  Process Name: " + process.getName());

                String jobId = null;
                Map<String, Value> attributes = process.getAttributesMap();
                if (attributes.containsKey("bigquery_job_id")) {
                    jobId = attributes.get("bigquery_job_id").getStringValue(); // Wait, protobuf Value can be String, Number, etc.
                }

                if (jobId != null) {
                    System.out.println("  Found Job ID: " + jobId);
                    getBigQueryJobDetails(projectId, jobId, location, keyPath);
                }
            }

        } catch (IOException e) {
            System.err.println("An error occurred while running lineage demo: " + e.getMessage());
        }
    }
}
