# Data Lakehouse Installation Guide

This guide provides instructions for installing and configuring the Data Lakehouse backend service.

## Prerequisites

- Java 11 or later
- Apache Hadoop 3.3.x
- Apache Spark 3.4.x
- Apache Kafka 3.5.x
- Trino 426 or later
- HDFS configured and running
- Hadoop YARN configured and running (for Spark jobs)

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/goldmansachsinnovationcenter/self-service-scheduler.git
cd self-service-scheduler
```

### 2. Build the Project

```bash
mvn clean package
```

### 3. Configure HDFS

Ensure HDFS is running and create the necessary directories:

```bash
hdfs dfs -mkdir -p /warehouse
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /checkpoints
hdfs dfs -chmod -R 777 /warehouse
hdfs dfs -chmod -R 777 /user/hive/warehouse
hdfs dfs -chmod -R 777 /checkpoints
```

### 4. Configure Hive Metastore (Optional)

If using Hive Metastore, ensure it's running and properly configured.

### 5. Configure Trino

Install Trino and configure it to use the Iceberg catalog:

1. Download and extract Trino:
   ```bash
   wget https://repo1.maven.org/maven2/io/trino/trino-server/426/trino-server-426.tar.gz
   tar -xzf trino-server-426.tar.gz
   ```

2. Create a catalog configuration for Iceberg:
   ```bash
   mkdir -p trino-server-426/etc/catalog
   ```

3. Create `trino-server-426/etc/catalog/iceberg.properties`:
   ```properties
   connector.name=iceberg
   hive.metastore.uri=thrift://localhost:9083
   iceberg.catalog.type=hadoop
   iceberg.warehouse=hdfs://localhost:9000/warehouse
   ```

4. Start Trino:
   ```bash
   trino-server-426/bin/launcher start
   ```

### 6. Configure Kafka

Ensure Kafka is running and create the necessary topics:

```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic data-lakehouse-topic
```

### 7. Deploy the API Service

```bash
java -jar api/target/api-1.0.0-SNAPSHOT.jar
```

### 8. Deploy the Spark Module

Submit the Spark job to YARN:

```bash
spark-submit \
  --class com.gs.datalakehouse.spark.job.KafkaToIcebergJob \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  spark-module/target/spark-module-1.0.0-SNAPSHOT.jar \
  localhost:9092 data-lakehouse-topic hadoop_catalog default.table_name
```

## Verification

1. Verify the API service is running:
   ```bash
   curl http://localhost:8080/api/tables/default
   ```

2. Verify Trino can query the Iceberg tables:
   ```bash
   trino --server localhost:8080 --catalog iceberg --schema default
   ```

3. Run a sample query:
   ```sql
   SELECT * FROM iceberg.default.table_name LIMIT 10;
   ```

## Troubleshooting

- Check the logs in the `logs` directory for any errors
- Verify that all services (HDFS, Kafka, Trino) are running
- Ensure proper permissions on HDFS directories
- Check Spark job status in the YARN Resource Manager UI
