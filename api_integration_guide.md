# 📚 Enterprise API Integration Guide: BigQuery & Dataplex Data Lineage

This guide explains how to integrate your custom tools with Google Cloud services using Java client libraries for BigQuery and Dataplex Data Lineage. This documentation is intended to teach developers how to use these APIs programmatically.

---

## 🏗️ Project Setup (Maven)

To connect to these services, you need to add the standard Google Cloud BOM and client libraries to your `pom.xml`:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.google.cloud</groupId>
            <artifactId>libraries-bom</artifactId>
            <version>26.54.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- BigQuery Client -->
    <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>google-cloud-bigquery</artifactId>
    </dependency>
    <!-- Data Lineage Client -->
    <dependency>
        <groupId>com.google.cloud</groupId>
        <artifactId>google-cloud-datalineage</artifactId>
    </dependency>
</dependencies>
```

---

## 🔍 1. BigQuery Schema Extraction

The BigQuery API allows you to programmatically inspect datasets and tables to retrieve column-level metadata.

### 🔑 Key Classes Used:
- `BigQuery`: The main client interface to BigQuery.
- `Dataset`: Represents a collection of tables.
- `Table`: Represents a BigQuery table.
- `Field`: Represents a column inside a table schema.

### 💻 Code Explanation (`BigQuerySchemaExtractor.java`)

#### Step 1: Initialize Client
We initialize the BigQuery client using standard options. It will automatically load credentials from Environment Variables (Application Default Credentials).

```java
BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
```

#### Step 2: List Datasets & Tables
We can either fetch a specific dataset or list all datasets in a project:

```java
// List all datasets if none specified
Iterable<Dataset> datasets = bigquery.listDatasets(projectId).iterateAll();

for (Dataset dataset : datasets) {
    // List tables within this dataset
    Iterable<Table> tables = bigquery.listTables(dataset.getDatasetId()).iterateAll();
}
```

#### Step 3: Print Schema (Fields)
To get the actual columns, we retrieve the `getSchema()` from `TableDefinition`:

```java
Table table = bigquery.getTable(tableItem.getTableId()); // Fetch full definition
if (table.getDefinition().getSchema() != null) {
    for (Field field : table.getDefinition().getSchema().getFields()) {
        System.out.println("Col: " + field.getName() + " [" + field.getType() + "]");
    }
}
```

---

## 🔗 2. Dataplex Data Lineage Graph

Dataplex Data Lineage tracks how data moves between tables. It links source tables to target tables via **Processes** (often a BigQuery SQL job).

### 🔑 Key Classes Used:
- `LineageClient`: The main client interface to Dataplex Lineage.
- `SearchLinksRequest`: API call to query for links by target entity.
- `BatchSearchLinkProcessesRequest`: API call to query process details (history) mapped to links.

### 💻 Code Explanation (`DataLineageGraphExtractor.java`)

#### Step 1: Search Lineage Links
Lineage is queried using a standard target URI format: `bigquery:project_id.dataset_id.table_id`.

```java
try (LineageClient lineageClient = LineageClient.create()) {
    SearchLinksRequest request = SearchLinksRequest.newBuilder()
            .setParent("projects/" + projectId + "/locations/" + location)
            .setTarget(EntityReference.newBuilder().setFullyQualifiedName("bigquery:project.dataset.table").build())
            .build();

    LineageClient.SearchLinksPagedResponse response = lineageClient.searchLinks(request);
}
```

#### Step 2: Resolve Upstream SQL Jobs (Processes)
Links are tied to Processes which contain history metadata. We batch scan them to resolve find what SQL actually ran:

```java
BatchSearchLinkProcessesRequest processRequest = BatchSearchLinkProcessesRequest.newBuilder()
        .setParent(parent)
        .addAllLinks(linkNames) // List of link names collected in Step 1
        .build();

LineageClient.BatchSearchLinkProcessesPagedResponse processLinksList = lineageClient.batchSearchLinkProcesses(processRequest);
```

#### Step 3: Link to BigQuery Job History
The process metadata often contains the `bigquery_job_id` attribute. You can use it to fetch job details:

```java
for (ProcessLinks processLink : processLinksList.iterateAll()) {
    Process process = lineageClient.getProcess(processLink.getProcess());
    Map<String, Value> attributes = process.getAttributesMap();
    
    if (attributes.containsKey("bigquery_job_id")) {
        String jobId = attributes.get("bigquery_job_id").getStringValue();
        // Query BigQuery job history using this jobId!
    }
}
```

---

## 🌐 3. 原生 REST API 参考 (Native REST API Reference)

本节列出上述 Java/Python SDK 方法底层对应的原生 Google Cloud REST API。这对于使用其他语言（如 Go、NodeJS）或直接 HTTP 调用的开发者非常有用。

### 📊 BigQuery REST API

#### 3.1 获取数据集列表 (List Datasets)
- **地址/接口**：`https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/datasets`
- **Java SDK**：`bigquery.listDatasets(projectId)`
- **Python SDK**：`client.list_datasets()`
- **请求方式**：`GET`
- **入参**：
  - `projectId` (路径参数): Google Cloud 项目 ID。
- **出参**：JSON 格式的数据集列表：
  ```json
  {
    "datasets": [
      {
        "id": "project-id:dataset_id",
        "datasetReference": { "projectId": "project-id", "datasetId": "dataset_id" },
        "location": "US"
      }
    ]
  }
  ```

#### 3.2 获取表列表 (List Tables)
- **地址/接口**：`https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables`
- **Java SDK**：`bigquery.listTables(datasetId)`
- **Python SDK**：`client.list_tables(datasetId)`
- **请求方式**：`GET`
- **入参**：
  - `projectId` (路径参数): 项目 ID。
  - `datasetId` (路径参数): 数据集 ID。
- **出参**：JSON 格式的表列表：
  ```json
  {
    "tables": [
      {
        "id": "project-id:dataset_id.table_id",
        "tableReference": { "projectId": "project-id", "datasetId": "dataset_id", "tableId": "table_id" }
      }
    ]
  }
  ```

#### 3.3 获取单表结构 (Get Table Schema)
- **地址/接口**：`https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}`
- **Java SDK**：`bigquery.getTable(tableId)`
- **Python SDK**：`client.get_table(table_ref)`
- **请求方式**：`GET`
- **入参**：
  - `projectId`, `datasetId`, `tableId` (路径参数)。
- **出参**：JSON 格式的表结构：
  ```json
  {
    "kind": "bigquery#table",
    "id": "project-id:dataset_id.table_id",
    "schema": {
      "fields": [
        { "name": "id", "type": "STRING", "mode": "NULLABLE", "description": "Unique ID" },
        { "name": "value", "type": "INTEGER", "mode": "NULLABLE" }
      ]
    }
  }
  ```

#### 3.4 获取 BigQuery Job 详情 (Get Job)
- **地址/接口**：`https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/jobs/{jobId}`
- **Java SDK**：`bigquery.getJob(jobId)`
- **Python SDK**：`client.get_job(job_id)`
- **请求方式**：`GET`
- **入参**：
  - `projectId`, `jobId` (路径参数)。
  - `location` (查询参数): Job 运行的地理位置（如 `us`）。
- **出参**：JSON 格式的 Job 详情：
  ```json
  {
    "kind": "bigquery#job",
    "id": "project-id:job_id",
    "statistics": { "startTime": "...", "endTime": "..." },
    "configuration": {
      "query": {
        "query": "SELECT * FROM my_table",
        "destinationTable": { "projectId": "...", "datasetId": "...", "tableId": "..." }
      }
    }
  }
  ```

---

### 🧬 Dataplex Data Lineage REST API

#### 3.4 搜索血缘链接 (Search Links)
- **地址/接口**：`https://datalineage.googleapis.com/v1/projects/{projectId}/locations/{location}/links:search`
- **Java SDK**：`lineageClient.searchLinks(request)`
- **Python SDK**：`client.search_links(request)`
- **请求方式**：`POST`
- **入参**：
  - `projectId`, `location` (路径参数)。
  - `requestBody` (JSON):
    ```json
    {
      "target": {
        "fullyQualifiedName": "bigquery:project_id.dataset_id.table_id"
      }
    }
    ```
- **出参**：JSON 格式的血缘链接列表：
  ```json
  {
    "links": [
      {
        "name": "projects/my-project/locations/us/links/123",
        "source": { "fullyQualifiedName": "bigquery:my-project.my_dataset.source_table" },
        "target": { "fullyQualifiedName": "bigquery:my-project.my_dataset.target_table" },
        "startTime": "2026-03-31T10:00:00Z"
      }
    ]
  }
  ```

#### 3.5 批量搜索血缘进程 (Batch Search Link Processes)
- **地址/接口**：`https://datalineage.googleapis.com/v1/projects/{projectId}/locations/{location}/processes:batchSearchLink`
- **Java SDK**：`lineageClient.batchSearchLinkProcesses(request)`
- **Python SDK**：`client.batch_search_link_processes(request)`
- **请求方式**：`POST`
- **入参**：
  - `projectId`, `location` (路径参数)。
  - `requestBody` (JSON):
    ```json
    {
      "links": ["projects/.../locations/.../links/..."]
    }
    ```
- **出参**：JSON 格式的进程映射列表：
  ```json
  {
    "processLinks": [
      {
        "name": "projects/my-project/locations/us/processLinks/456",
        "process": "projects/my-project/locations/us/processes/bq-job-123"
      }
    ]
  }
  ```

#### 3.6 获取单个血缘进程详情 (Get Process)
- **地址/接口**：`https://datalineage.googleapis.com/v1/projects/{projectId}/locations/{location}/processes/{processId}`
- **Java SDK**：`lineageClient.getProcess(processId)`
- **Python SDK**：`client.get_process(process_id)`
- **请求方式**：`GET`
- **入参**：
  - `projectId`, `location`, `processId` (路径参数)。
- **出参**：JSON 格式的进程详情：
  ```json
  {
    "name": "projects/my-project/locations/us/processes/bq-job-123",
    "displayName": "BigQuery Job Run",
    "attributes": {
      "bigquery_job_id": {
        "stringValue": "bq-job-123"
      }
    }
  }

---

## ☕ 4. BigQuery JDBC 访问机制 (BigQuery JDBC Access Mechanism)

在某些情况下，传统的 SDK（基于 REST/gRPC）可能与项目现有的依赖（如 `libraries-bom`）发生冲突。此时使用 **JDBC 驱动隔离方案** 是最佳实践。

### 🔑 核心类与驱动：
- **Driver Class**: `com.google.cloud.bigquery.jdbc.BigQueryDriver`
- **Connection URL**: `jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId={项目ID};OAuthType=0;OAuthPvtKeyPath={密钥文件路径}`

### 💻 隔离项目配置 (Isolated `pom.xml`)

为了避免依赖冲突，建议将 JDBC Demo 放到一个独立的子模块中，并在 `maven-shade-plugin` 中过滤签名文件：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.6.0</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
                <filters>
                    <filter>
                        <artifact>*:*</artifact>
                        <excludes>
                            <exclude>META-INF/*.SF</exclude>
                            <exclude>META-INF/*.DSA</exclude>
                            <exclude>META-INF/*.RSA</exclude>
                        </excludes>
                    </filter>
                </filters>
            </configuration>
        </execution>
    </executions>
</plugin>
```

### 💻 代码示例：读取 Metadata 与 表描述 (Reading Metadata and Descriptions)

使用 `ResultSetMetaData` 可以动态读取任意查询（如 `SELECT *` 或 `INFORMATION_SCHEMA`）的所有列，而无需硬编码列名：

```java
String sql = "SELECT * FROM `llm_demo.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'mihoyo_features_with_id'";

try (Connection conn = DriverManager.getConnection(url);
     Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery(sql)) {

    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    while (rs.next()) {
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnName(i) + ": " + rs.getString(i) + " | ");
        }
        System.out.println();
    }
}
```

> [!TIP]
> querying `INFORMATION_SCHEMA.TABLES` allows you to extract **DDL statements** and **Table Descriptions** defined in BigQuery!
  ```




