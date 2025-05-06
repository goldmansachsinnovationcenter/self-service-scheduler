# Data Lakehouse Integration Guide

This guide provides instructions for integrating with the Data Lakehouse backend service.

## Integration Methods

The Data Lakehouse service provides multiple methods for accessing and querying data:

1. REST API
2. JDBC Connection
3. Trino CLI
4. Direct HDFS Access

## REST API

The Data Lakehouse exposes a REST API for managing tables and executing queries.

### API Endpoints

#### Tables API

- **Create Table**: `POST /api/tables`
  ```bash
  curl -X POST http://localhost:8080/api/tables \
    -H "Content-Type: application/json" \
    -d '{
      "name": "example_table",
      "database": "default",
      "location": "hdfs://localhost:9000/warehouse/default/example_table",
      "columns": [
        {
          "name": "id",
          "type": "string",
          "nullable": false,
          "comment": "Unique identifier"
        },
        {
          "name": "timestamp",
          "type": "timestamp",
          "nullable": true,
          "comment": "Event timestamp"
        },
        {
          "name": "data",
          "type": "string",
          "nullable": true,
          "comment": "Event data"
        }
      ],
      "partitionBy": ["timestamp"],
      "properties": {
        "format-version": "2",
        "write.format.default": "parquet"
      },
      "format": "parquet",
      "description": "Example table for demonstration"
    }'
  ```

- **Get Table**: `GET /api/tables/{database}/{name}`
  ```bash
  curl -X GET http://localhost:8080/api/tables/default/example_table
  ```

- **List Tables**: `GET /api/tables/{database}`
  ```bash
  curl -X GET http://localhost:8080/api/tables/default
  ```

- **Update Table**: `PUT /api/tables/{database}/{name}`
  ```bash
  curl -X PUT http://localhost:8080/api/tables/default/example_table \
    -H "Content-Type: application/json" \
    -d '{
      "name": "example_table",
      "database": "default",
      "location": "hdfs://localhost:9000/warehouse/default/example_table",
      "columns": [
        {
          "name": "id",
          "type": "string",
          "nullable": false,
          "comment": "Unique identifier"
        },
        {
          "name": "timestamp",
          "type": "timestamp",
          "nullable": true,
          "comment": "Event timestamp"
        },
        {
          "name": "data",
          "type": "string",
          "nullable": true,
          "comment": "Event data"
        },
        {
          "name": "additional_field",
          "type": "string",
          "nullable": true,
          "comment": "Additional data field"
        }
      ],
      "partitionBy": ["timestamp"],
      "properties": {
        "format-version": "2",
        "write.format.default": "parquet"
      },
      "format": "parquet",
      "description": "Updated example table"
    }'
  ```

- **Delete Table**: `DELETE /api/tables/{database}/{name}`
  ```bash
  curl -X DELETE http://localhost:8080/api/tables/default/example_table
  ```

#### Query API

- **Execute Query**: `POST /api/query`
  ```bash
  curl -X POST http://localhost:8080/api/query \
    -H "Content-Type: application/json" \
    -d '{
      "sql": "SELECT * FROM default.example_table LIMIT 10"
    }'
  ```

## JDBC Connection

You can connect to the data lakehouse using the Trino JDBC driver:

```java
import java.sql.*;
import java.util.Properties;

public class TrinoJdbcExample {
    public static void main(String[] args) throws SQLException {
        // Connection parameters
        String url = "jdbc:trino://localhost:8080/iceberg/default";
        Properties properties = new Properties();
        properties.setProperty("user", "trino");
        
        // Connect to Trino
        Connection connection = DriverManager.getConnection(url, properties);
        
        // Execute a query
        try (Statement statement = connection.createStatement()) {
            String sql = "SELECT * FROM example_table LIMIT 10";
            ResultSet resultSet = statement.executeQuery(sql);
            
            // Process results
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            // Print column names
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i) + "\t");
            }
            System.out.println();
            
            // Print rows
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getObject(i) + "\t");
                }
                System.out.println();
            }
        }
        
        // Close the connection
        connection.close();
    }
}
```

## Trino CLI

You can use the Trino CLI to query data:

```bash
# Download Trino CLI
wget https://repo1.maven.org/maven2/io/trino/trino-cli/426/trino-cli-426-executable.jar -O trino
chmod +x trino

# Connect to Trino
./trino --server localhost:8080 --catalog iceberg --schema default

# Execute queries
trino> SELECT * FROM example_table LIMIT 10;
```

## Direct HDFS Access

You can directly access the Parquet files in HDFS:

```bash
# List files in HDFS
hdfs dfs -ls /warehouse/default/example_table

# Copy files locally
hdfs dfs -get /warehouse/default/example_table/data.parquet .

# Use Parquet tools to inspect files
java -jar parquet-tools-1.12.0.jar schema data.parquet
java -jar parquet-tools-1.12.0.jar head data.parquet
```

## Integration with Spark

You can read and write Iceberg tables using Spark:

```scala
// Initialize Spark session with Iceberg
val spark = SparkSession.builder()
  .appName("Iceberg Integration")
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
  .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://localhost:9000/warehouse")
  .getOrCreate()

// Read from Iceberg table
val df = spark.read.format("iceberg").load("hadoop_catalog.default.example_table")
df.show()

// Write to Iceberg table
df.write
  .format("iceberg")
  .mode("append")
  .save("hadoop_catalog.default.example_table")
```

## Security Considerations

- Ensure proper authentication and authorization for API access
- Configure Trino with appropriate security settings
- Set up Kerberos authentication for HDFS if needed
- Use SSL/TLS for secure communication
