package com.customer.jt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BigQueryJDBCExample {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java com.customer.jt.BigQueryJDBCExample <ProjectId> <ServiceAccountKeyPath>");
            System.out.println("Example: java com.customer.jt.BigQueryJDBCExample binggang-lab /path/to/sa-key.json");
            System.exit(1);
        }

        String projectId = args[0];
        String keyPath = args[1];

        // Connection URL format for Google BigQuery JDBC driver
        String url = String.format("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=%s;OAuthType=0;OAuthPvtKeyPath=%s", 
                projectId, keyPath);

        System.out.println("Connecting to BigQuery via JDBC (Isolated Project)...");
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            System.out.println("Connection successful! Executing query...");

            // User requested query (Restored now that data access is verified)
            String sql = "SELECT * FROM `llm_demo.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'mihoyo_features_with_id'";
            System.out.println("Query: " + sql);

            try (ResultSet rs = stmt.executeQuery(sql)) {
                java.sql.ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                System.out.println("\nQuery Results:");
                System.out.println("----------------------------------------");
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(metaData.getColumnName(i) + ": " + rs.getString(i) + " | ");
                    }
                    System.out.println();
                }
                System.out.println("----------------------------------------");
            }

        } catch (SQLException e) {
            System.err.println("JDBC Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
