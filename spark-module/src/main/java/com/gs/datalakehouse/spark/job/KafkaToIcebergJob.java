package com.gs.datalakehouse.spark.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Spark job for ingesting data from Kafka to Iceberg tables.
 */
public class KafkaToIcebergJob {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToIcebergJob.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KafkaToIcebergJob <kafka-bootstrap-servers> <topic> <iceberg-catalog> <table-name>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        String catalog = args[2];
        String tableName = args[3];

        SparkSession spark = SparkSession.builder()
                .appName("Kafka to Iceberg Ingestion")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog." + catalog, "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + catalog + ".type", "hadoop")
                .config("spark.sql.catalog." + catalog + ".warehouse", "hdfs://localhost:9000/warehouse")
                .getOrCreate();

        Dataset<Row> kafkaDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> processedDF = processKafkaData(spark, kafkaDF);

        StreamingQuery query = processedDF
                .writeStream()
                .format("iceberg")
                .outputMode("append")
                .option("path", catalog + "." + tableName)
                .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/" + tableName)
                .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
                .start();

        query.awaitTermination();
    }

    /**
     * Process the Kafka data before writing to Iceberg.
     * This method should be customized based on the specific data format and schema.
     */
    private static Dataset<Row> processKafkaData(SparkSession spark, Dataset<Row> kafkaDF) {
        Dataset<Row> valueDF = kafkaDF.selectExpr("CAST(value AS STRING) as json_value");

        StructType schema = new StructType()
                .add("id", "string")
                .add("timestamp", "timestamp")
                .add("data", "string");

        Dataset<Row> parsedDF = valueDF.selectExpr(
                "from_json(json_value, '" + schema.json() + "') as parsed_data")
                .select("parsed_data.*");

        Dataset<Row> transformedDF = parsedDF
                .withColumnRenamed("id", "record_id")
                .withColumnRenamed("timestamp", "event_time");

        return transformedDF;
    }
}
