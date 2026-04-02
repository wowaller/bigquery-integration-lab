package com.customer.jt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * BigQueryJDBCExample demonstrates how to connect to Google BigQuery using the JDBC driver
 * and execute a query. This example uses Service Account Authentication.
 */
public class BigQueryJDBCExample {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java com.customer.jt.BigQueryJDBCExample <ProjectId> <ServiceAccountKeyPath>");
            System.err.println("Example: java com.customer.jt.BigQueryJDBCExample binggang-lab /path/to/sa-key.json");
            System.exit(1);
        }

        String projectId = args[0];
        String keyPath = args[1];

        // Connection string for Service Account authentication (OAuthType=0)
        // Note: Many driver versions can read the JSON file directly without explicit ServiceAcctEmail.
        String url = String.format(
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=%s;OAuthType=0;OAuthPvtKeyPath=%s;",
            projectId, keyPath
        );

        // Retrieve DDL for the specific table as requested by the user
        String query = "SELECT ddl FROM `llm_demo.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'mihoyo_features_with_id';";

        try {
            // Explicitly load the driver class for clarity
            Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");

            System.out.println("Connecting to BigQuery via JDBC...");
            try (Connection conn = DriverManager.getConnection(url);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                System.out.println("Query executed successfully. Results:");
                boolean found = false;
                while (rs.next()) {
                    found = true;
                    System.out.println("==================================================");
                    System.out.println("Table DDL:");
                    System.out.println(rs.getString("ddl"));
                    System.out.println("==================================================");
                }
                if (!found) {
                    System.out.println("No DDL found for table 'mihoyo_features_with_id' in 'llm_demo'. Verify table name and dataset.");
                }
            }
        } catch (Exception e) {
            System.err.println("JDBC Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
