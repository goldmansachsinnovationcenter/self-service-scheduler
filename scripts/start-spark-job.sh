

if [ "$#" -lt 4 ]; then
    echo "Usage: $0 <kafka-bootstrap-servers> <kafka-topic> <iceberg-catalog> <table-name>"
    echo "Example: $0 localhost:9092 data-lakehouse-topic hadoop_catalog default.example_table"
    exit 1
fi

KAFKA_BOOTSTRAP_SERVERS=$1
KAFKA_TOPIC=$2
ICEBERG_CATALOG=$3
TABLE_NAME=$4

export JAVA_HOME=/usr/lib/jvm/java-11
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

echo "Submitting Kafka to Iceberg Spark job to YARN..."
spark-submit \
  --class com.gs.datalakehouse.spark.job.KafkaToIcebergJob \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=hdfs://localhost:9000/warehouse" \
  spark-module/target/spark-module-1.0.0-SNAPSHOT.jar \
  ${KAFKA_BOOTSTRAP_SERVERS} ${KAFKA_TOPIC} ${ICEBERG_CATALOG} ${TABLE_NAME}

echo "Spark job submitted. Check YARN Resource Manager UI for status."
