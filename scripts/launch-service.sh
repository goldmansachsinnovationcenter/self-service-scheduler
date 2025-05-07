
echo "Checking for existing processes on required ports..."
TRINO_PORT=8080
API_PORT=8081

kill_process_on_port() {
    local port=$1
    local pid=$(lsof -t -i:$port)
    if [ ! -z "$pid" ]; then
        echo "Killing process $pid on port $port"
        kill -9 $pid
    fi
}

kill_process_on_port $TRINO_PORT
kill_process_on_port $API_PORT

echo "Setting up the environment..."
./scripts/setup-environment.sh

echo "Loading dummy data..."
./scripts/load-dummy-data.sh

echo "Starting Trino service..."
trino-server start &
sleep 10  # Wait for Trino to start

echo "Starting Data Lakehouse API service..."
./scripts/start-api.sh &

echo "Data Lakehouse service started successfully."
echo "API is available at http://localhost:8081"
echo "Trino is available at http://localhost:8080"
echo ""
echo "Use the following commands to query data:"
echo "1. Via API: curl -X POST -H 'Content-Type: application/json' -d '{\"sql\":\"SELECT * FROM default.customers LIMIT 10\"}' http://localhost:8081/api/query"
echo "2. Via CLI: trino-cli --server localhost:8080 --catalog iceberg --schema default"
echo ""
echo "Sample queries:"
echo "SELECT * FROM customers LIMIT 10;"
echo "SELECT * FROM transactions LIMIT 10;"
echo "SELECT * FROM products LIMIT 10;"
echo "SELECT c.name, t.amount, p.name as product_name FROM customers c JOIN transactions t ON c.customer_id = t.customer_id JOIN products p ON t.product_id = p.product_id LIMIT 10;"
