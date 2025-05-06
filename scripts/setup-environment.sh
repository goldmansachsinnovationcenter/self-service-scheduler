

echo "Setting up Data Lakehouse environment..."

echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /warehouse
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /checkpoints
hdfs dfs -chmod -R 777 /warehouse
hdfs dfs -chmod -R 777 /user/hive/warehouse
hdfs dfs -chmod -R 777 /checkpoints

echo "Creating Kafka topics..."
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic data-lakehouse-topic

echo "Environment setup completed."
