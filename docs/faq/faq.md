# Data Lakehouse FAQ

## General Questions

### What is a Data Lakehouse?

A Data Lakehouse is a modern data architecture that combines the best features of data lakes and data warehouses. It provides:

- Low-cost storage in open formats (like data lakes)
- ACID transactions and schema enforcement (like data warehouses)
- Support for diverse workloads including BI, SQL analytics, and machine learning
- Direct access to source data without ETL

Our implementation uses Apache Iceberg as the table format, HDFS as the storage layer, Apache Spark for data processing, and Trino for SQL queries.

### What are the key components of this Data Lakehouse service?

Our Data Lakehouse service consists of:

1. **Apache Iceberg**: Table format that provides ACID transactions, schema evolution, and time travel
2. **HDFS**: Distributed file system for storing data files
3. **Apache Spark**: Processing engine for data ingestion and transformation
4. **Trino**: SQL query engine for interactive analytics
5. **Parquet**: Columnar storage format for efficient data storage and retrieval
6. **Hadoop Catalog**: Metadata management for Iceberg tables
7. **Kafka Integration**: For streaming data ingestion

### What data formats are supported?

The primary data format is Parquet, which provides efficient columnar storage with compression. However, the system can ingest data from various sources and formats, including:

- JSON from Kafka
- CSV files
- Avro files
- Parquet files
- Database tables via JDBC

## Technical Questions

### How do I create a new table?

You can create a new table using the REST API:

```bash
curl -X POST http://localhost:8080/api/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_table",
    "database": "default",
    "columns": [
      {
        "name": "id",
        "type": "string",
        "nullable": false
      },
      {
        "name": "value",
        "type": "int",
        "nullable": true
      }
    ],
    "partitionBy": ["id"],
    "format": "parquet"
  }'
```

### How do I query data?

You have several options:

1. **REST API**:
   ```bash
   curl -X POST http://localhost:8080/api/query \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT * FROM default.my_table LIMIT 10"}'
   ```

2. **JDBC**:
   ```java
   String url = "jdbc:trino://localhost:8080/iceberg/default";
   Connection conn = DriverManager.getConnection(url, "trino", null);
   Statement stmt = conn.createStatement();
   ResultSet rs = stmt.executeQuery("SELECT * FROM my_table LIMIT 10");
   ```

3. **Trino CLI**:
   ```bash
   ./trino --server localhost:8080 --catalog iceberg --schema default
   trino> SELECT * FROM my_table LIMIT 10;
   ```

### How do I ingest data from Kafka?

Data ingestion from Kafka is handled by the Spark module. You can submit a Spark job:

```bash
spark-submit \
  --class com.gs.datalakehouse.spark.job.KafkaToIcebergJob \
  --master yarn \
  --deploy-mode cluster \
  spark-module/target/spark-module-1.0.0-SNAPSHOT.jar \
  localhost:9092 my-kafka-topic hadoop_catalog default.my_table
```

### How do I handle schema evolution?

Apache Iceberg supports schema evolution. You can add, rename, or drop columns using the API:

```bash
curl -X PUT http://localhost:8080/api/tables/default/my_table \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_table",
    "database": "default",
    "columns": [
      {
        "name": "id",
        "type": "string",
        "nullable": false
      },
      {
        "name": "value",
        "type": "int",
        "nullable": true
      },
      {
        "name": "new_column",
        "type": "string",
        "nullable": true
      }
    ],
    "partitionBy": ["id"],
    "format": "parquet"
  }'
```

### How do I monitor the system?

You can monitor:

1. **API Service**: Access Spring Boot Actuator endpoints at `/actuator/health`, `/actuator/metrics`, etc.
2. **Spark Jobs**: Use the YARN Resource Manager UI (typically at http://yarn-resource-manager:8088)
3. **Trino**: Access the Trino UI (typically at http://trino-coordinator:8080)
4. **HDFS**: Use the HDFS NameNode UI (typically at http://namenode:9870)

### How do I handle partitioning?

Specify partition columns when creating a table:

```bash
curl -X POST http://localhost:8080/api/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_table",
    "database": "default",
    "columns": [...],
    "partitionBy": ["date", "region"],
    "format": "parquet"
  }'
```

## Troubleshooting

### I'm getting "Table not found" errors

Check:
1. The table exists in the catalog
2. You're using the correct database and table name
3. The Iceberg catalog is properly configured

### Spark job fails with Kafka connection issues

Check:
1. Kafka is running and accessible
2. The topic exists
3. The bootstrap servers are correctly specified

### Queries are slow

Consider:
1. Optimizing partitioning
2. Adding appropriate file sizing (target-file-size-bytes property)
3. Running compaction to optimize small files
4. Increasing Trino resources

### I can't connect to the API

Check:
1. The API service is running
2. Network connectivity to the API host
3. Firewall settings

### How do I recover from failed transactions?

Iceberg automatically handles transaction failures. If a write fails, the table will remain in its previous valid state. You can:

1. Check the table's history: `SELECT * FROM iceberg.default."my_table$history"`
2. Time travel to a previous version if needed: `SELECT * FROM iceberg.default.my_table FOR VERSION AS OF 123`
