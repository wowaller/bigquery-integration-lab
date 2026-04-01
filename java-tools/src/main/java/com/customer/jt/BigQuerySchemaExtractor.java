package com.customer.jt;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Collections;

/**
 * BigQuerySchemaExtractor is a utility tool to extract and print schema information
 * for BigQuery tables. It uses the Google Cloud BigQuery client library.
 * It can list all datasets and tables in a project or filter by specific IDs.
 */
public class BigQuerySchemaExtractor {

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("p", "project", true, "Google Cloud project ID");
        options.addOption("d", "dataset", true, "BigQuery dataset ID (optional)");
        options.addOption("t", "table", true, "BigQuery table ID (optional)");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String projectId = cmd.getOptionValue("project");
            String datasetIdArg = cmd.getOptionValue("dataset");
            String tableIdArg = cmd.getOptionValue("table");

            if (projectId == null) {
                System.err.println("Usage: java BigQuerySchemaExtractor -p PROJECT_ID [-d DATASET_ID] [-t TABLE_ID]");
                System.exit(1);
            }

            getSchemaInfo(projectId, datasetIdArg, tableIdArg);

        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Extracts and prints schema details for datasets and tables in a project.
     * It uses the BigQuery Client to list datasets and tables, and prints their
     * column names, types, and descriptions.
     * 
     * @param projectId The active Google Cloud Project ID.
     * @param datasetId Optional. If provided, filters for this specific dataset.
     * @param tableId Optional. If provided, filters for this specific table.
     */
    public static void getSchemaInfo(String projectId, String datasetId, String tableId) {
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();

        Iterable<Dataset> datasets;
        if (datasetId != null) {
            Dataset dataset = bigquery.getDataset(DatasetId.of(projectId, datasetId));
            if (dataset == null) {
                System.err.println("Dataset not found: " + datasetId);
                return;
            }
            datasets = Collections.singletonList(dataset);
        } else {
            datasets = bigquery.listDatasets(projectId).iterateAll();
        }

        for (Dataset dataset : datasets) {
            System.out.println("Processing Dataset: " + dataset.getDatasetId().getDataset());

            Iterable<Table> tables;
            if (tableId != null) {
                Table table = bigquery.getTable(TableId.of(projectId, dataset.getDatasetId().getDataset(), tableId));
                if (table != null) {
                    tables = Collections.singletonList(table);
                } else {
                    System.err.println("Table not found: " + tableId + " in dataset " + dataset.getDatasetId().getDataset());
                    continue;
                }
            } else {
                tables = bigquery.listTables(dataset.getDatasetId()).iterateAll();
            }

            for (Table tableItem : tables) {
                Table table = bigquery.getTable(tableItem.getTableId());
                System.out.println("    Table: " + table.getTableId().getTable());
                System.out.println("      Description: " + table.getDescription());
                System.out.println("      Columns:");
                
                if (table.getDefinition().getSchema() != null) {
                    for (Field field : table.getDefinition().getSchema().getFields()) {
                        System.out.println("        - Name: " + field.getName());
                        System.out.println("          Type: " + field.getType());
                        System.out.println("          Mode: " + field.getMode());
                        System.out.println("          Description: " + field.getDescription());
                    }
                }
            }
        }
    }
}
