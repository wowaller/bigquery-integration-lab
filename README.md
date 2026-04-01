# 🚀 Enterprise Java & Python Tooling for BigQuery & Dataplex

This repository contains a suite of enterprise tools for Google Cloud BigQuery and Dataplex Data Lineage. It includes both Python and Java implementations for schema extraction, lineage graph visualization, and Airflow/Composer job monitoring.

---

## 📂 Project Structure

### 🛠️ Java Tools (`java-tools/`)
A Maven-based project containing standard Java utilities for enterprise tracking:
-   **`BigQuerySchemaExtractor.java`**: Lists datasets, tables, and views with their descriptions and column-level schemas.
-   **`DataLineageGraphExtractor.java`**: Queries Dataplex Data Lineage API to identify upstream SQL jobs and source tables for a given target entity.

### 🐍 Python Utilities (`python-tools/`)
Standalone Python scripts for automation and monitoring:
-   **`python-tools/monitor_composer_jobs.py`**: Monitors Airflow DAG runs using the Airflow REST API. Connects to `google-cloud-sdk` and lists job states efficiently.
-   **`python-tools/get_bigquery_schema.py`**: Python implementation of schema extraction.
-   **`python-tools/get_lineage.py`**: Python implementation of data lineage visualization.

### 📚 Documentation
-   **`api_integration_guide.md`**: Best-practices documentation for integrating with Google BigQuery and Dataplex APIs (Java/Python method mappings + sample JSON outputs).
-   **`cloud_composer_monitoring.md`**: How-to guide for monitoring Airflow DAG runs using `monitor_composer_jobs.py`.

---

## 🏗️ Build & Setup

### Java Compilation
Because some corporate environments require authentication setup in Login Shells, we recommend building using a Login context:

```bash
zsh -l -c "cd java-tools && mvn clean package"
```

This creates a "fat JAR" containing all dependencies at:
`java-tools/target/java-tools-1.0-SNAPSHOT.jar`

### Python Setup
Install dependencies listed inside the `requirements.txt`:
```bash
pip install -r requirements.txt
```

---

## ▶️ Running the Tools

### BigQuery Schema Extractor (Java)
To list **all datasets and tables** in a project:
```bash
zsh -l -c "java -cp java-tools/target/java-tools-1.0-SNAPSHOT.jar com.customer.jt.BigQuerySchemaExtractor -p YOUR_PROJECT_ID"
```

To filter for a specific dataset/table:
```bash
zsh -l -c "java -cp java-tools/target/java-tools-1.0-SNAPSHOT.jar com.customer.jt.BigQuerySchemaExtractor -p YOUR_PROJECT_ID -d DATASET_ID -t TABLE_ID"
```

### Data Lineage Graph Extractor (Java)
Find what SQL queries (processes) are modifying your tables:
```bash
zsh -l -c "java -cp java-tools/target/java-tools-1.0-SNAPSHOT.jar com.customer.jt.DataLineageGraphExtractor -p YOUR_PROJECT_ID -l us -d DATASET_ID -t TABLE_ID"
```

### Airflow Composer Monitoring (Python)
Checks recently run DAGs and their statuses within a 1-hour window:
```bash
python python-tools/monitor_composer_jobs.py -p YOUR_PROJECT_ID -l us-central1 -e composer-env-name --hours 1
```

---

## 🧹 Cleanups & Maintenance

You can run `git add` and `git commit` to start tracking your workspace as a clean repo! Standard temporary files (like `lineage_export.json` or local `.log` dumps) should be ignored or removed when sharing with customers.
