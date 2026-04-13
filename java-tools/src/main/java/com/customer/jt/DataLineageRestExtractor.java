package com.customer.jt;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * DataLineageRestExtractor extracts lineage information using the Data Lineage REST API directly,
 * without using Data Catalog API classes for response parsing.
 */
public class DataLineageRestExtractor {

    private static final String BASE_URL = "https://datalineage.googleapis.com/v1";

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("p", "project", true, "Google Cloud project ID");
        options.addOption("l", "location", true, "Region of the Data Catalog instance (e.g. us)");
        options.addOption("d", "dataset", true, "BigQuery dataset ID");
        options.addOption("t", "table", true, "BigQuery table ID");
        options.addOption("k", "key", true, "Service Account key path (optional)");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String projectId = cmd.getOptionValue("project");
            String location = cmd.getOptionValue("location");
            String datasetId = cmd.getOptionValue("dataset");
            String tableId = cmd.getOptionValue("table");
            String keyPath = cmd.getOptionValue("key");

            if (projectId == null || location == null || datasetId == null || tableId == null) {
                System.err.println("Usage: java DataLineageRestExtractor -p PROJECT_ID -l LOCATION -d DATASET_ID -t TABLE_ID [-k KEY_PATH]");
                System.exit(1);
            }

            runLineageDemo(projectId, location, datasetId, tableId, keyPath);

        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            System.exit(1);
        }
    }

    private static String getAccessToken(String keyPath) throws IOException {
        GoogleCredentials credentials;
        if (keyPath != null) {
            credentials = GoogleCredentials.fromStream(new FileInputStream(keyPath))
                    .createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"));
        } else {
            credentials = GoogleCredentials.getApplicationDefault()
                    .createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"));
        }
        credentials.refreshIfExpired();
        return credentials.getAccessToken().getTokenValue();
    }

    public static void runLineageDemo(String projectId, String location, String datasetId, String tableId, String keyPath) {
        try {
            String accessToken = getAccessToken(keyPath);
            HttpClient client = HttpClient.newHttpClient();

            String target = String.format("bigquery:%s.%s.%s", projectId, datasetId, tableId);
            String parent = String.format("projects/%s/locations/%s", projectId, location);

            System.out.println("\n1. Searching for table lineage links with target: " + target);
            
            String searchLinksUrl = String.format("%s/%s:searchLinks", BASE_URL, parent);
            String searchLinksPayload = String.format("{\"target\": {\"fullyQualifiedName\": \"%s\"}}", target);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(searchLinksUrl))
                    .header("Authorization", "Bearer " + accessToken)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(searchLinksPayload))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.err.println("Failed to search links. Status code: " + response.statusCode());
                System.err.println("Response: " + response.body());
                return;
            }

            JsonObject searchLinksObj = JsonParser.parseString(response.body()).getAsJsonObject();
            JsonArray linksArray = searchLinksObj.getAsJsonArray("links");
            
            List<String> links = new ArrayList<>();
            if (linksArray != null) {
                linksArray.forEach(element -> links.add(element.getAsJsonObject().get("name").getAsString()));
            }

            if (links.isEmpty()) {
                System.out.println("No lineage links found.");
                return;
            }

            System.out.println("Found " + links.size() + " links. Fetching processes...");

            // 2. Batch Search Link Processes
            String batchSearchUrl = String.format("%s/%s:batchSearchLinkProcesses", BASE_URL, parent);
            
            // Construct JSON array for links using Gson
            JsonArray linksJsonArray = new JsonArray();
            links.forEach(linksJsonArray::add);
            
            JsonObject batchSearchPayloadObj = new JsonObject();
            batchSearchPayloadObj.add("links", linksJsonArray);
            String batchSearchPayload = batchSearchPayloadObj.toString();

            request = HttpRequest.newBuilder()
                    .uri(URI.create(batchSearchUrl))
                    .header("Authorization", "Bearer " + accessToken)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(batchSearchPayload))
                    .build();

            response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.err.println("Failed to batch search link processes. Status code: " + response.statusCode());
                System.err.println("Response: " + response.body());
                return;
            }

            JsonObject batchResponseObj = JsonParser.parseString(response.body()).getAsJsonObject();
            JsonArray processLinksArray = batchResponseObj.getAsJsonArray("processLinks");

            if (processLinksArray != null) {
                for (JsonElement element : processLinksArray) {
                    JsonObject processLinkObj = element.getAsJsonObject();
                    JsonArray linkDetailsArray = processLinkObj.getAsJsonArray("links");
                    
                    String linkName = "N/A";
                    if (linkDetailsArray != null && linkDetailsArray.size() > 0) {
                        linkName = linkDetailsArray.get(0).getAsJsonObject().get("link").getAsString();
                    }
                    
                    System.out.println("\nDetails for link: " + linkName);
                    String processName = processLinkObj.get("process").getAsString();
                    
                    // 3. Get Process details
                    String getProcessUrl = String.format("%s/%s", BASE_URL, processName);
                    
                    request = HttpRequest.newBuilder()
                            .uri(URI.create(getProcessUrl))
                            .header("Authorization", "Bearer " + accessToken)
                            .GET()
                            .build();

                    response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() != 200) {
                        System.err.println("Failed to get process. Status code: " + response.statusCode());
                        System.err.println("Response: " + response.body());
                        continue;
                    }

                    JsonObject processObj = JsonParser.parseString(response.body()).getAsJsonObject();
                    System.out.println("  Process Name: " + processObj.get("name").getAsString());

                    String jobId = null;
                    JsonObject attributes = processObj.getAsJsonObject("attributes");
                    if (attributes != null && attributes.has("bigquery_job_id")) {
                        jobId = attributes.get("bigquery_job_id").getAsString();
                    }

                    if (jobId != null) {
                        System.out.println("  Found Job ID: " + jobId);
                        System.out.println("  (BigQuery job details extraction omitted to keep example focused on Lineage REST API)");
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
