
echo "Loading dummy data into the data lakehouse..."

hdfs dfs -mkdir -p /warehouse/default/customers
hdfs dfs -mkdir -p /warehouse/default/transactions
hdfs dfs -mkdir -p /warehouse/default/products
hdfs dfs -chmod -R 777 /warehouse

echo "Generating customer data..."
spark-submit \
  --class com.gs.datalakehouse.spark.job.DummyDataGenerator \
  --master local[*] \
  spark-module/target/spark-module-1.0.0-SNAPSHOT.jar \
  --table customers \
  --count 1000

echo "Generating transaction data..."
spark-submit \
  --class com.gs.datalakehouse.spark.job.DummyDataGenerator \
  --master local[*] \
  spark-module/target/spark-module-1.0.0-SNAPSHOT.jar \
  --table transactions \
  --count 5000

echo "Generating product data..."
spark-submit \
  --class com.gs.datalakehouse.spark.job.DummyDataGenerator \
  --master local[*] \
  spark-module/target/spark-module-1.0.0-SNAPSHOT.jar \
  --table products \
  --count 200

echo "Dummy data loaded successfully."
