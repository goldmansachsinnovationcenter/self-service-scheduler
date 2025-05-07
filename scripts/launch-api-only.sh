
echo "Checking for existing processes on required ports..."
API_PORT=8081

kill_process_on_port() {
    local port=$1
    local pid=$(lsof -t -i:$port)
    if [ ! -z "$pid" ]; then
        echo "Killing process $pid on port $port"
        kill -9 $pid
    fi
}

kill_process_on_port $API_PORT

echo "Setting up the stubbed environment..."
rm -rf /tmp/stub-hdfs
mkdir -p /tmp/stub-hdfs/warehouse
mkdir -p /tmp/stub-hdfs/catalog

echo "Starting Data Lakehouse API service with stubbed HDFS..."
java -Dlogging.level.root=OFF -jar api/target/api-1.0.0-SNAPSHOT.jar --spring.profiles.active=stub-hdfs
