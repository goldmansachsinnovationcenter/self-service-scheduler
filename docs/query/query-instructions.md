# Data Lakehouse Query Instructions

This document provides instructions on how to query data from the Data Lakehouse service using both the API and CLI interfaces.

## Prerequisites

- Data Lakehouse service is running
- Dummy data has been loaded

## Querying via API

The Data Lakehouse service provides a REST API for executing SQL queries against the data. You can use the `/api/query` endpoint to execute SQL queries.

### Example using curl

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM default.customers LIMIT 10"}' \
  http://localhost:8081/api/query
```

### Example using Python

```python
import requests
import json

url = "http://localhost:8081/api/query"
headers = {"Content-Type": "application/json"}
data = {"sql": "SELECT * FROM default.customers LIMIT 10"}

response = requests.post(url, headers=headers, data=json.dumps(data))
results = response.json()

print(json.dumps(results, indent=2))
```

### Example using JavaScript

```javascript
fetch('http://localhost:8081/api/query', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    sql: 'SELECT * FROM default.customers LIMIT 10'
  })
})
.then(response => response.json())
.then(data => console.log(data))
.catch(error => console.error('Error:', error));
```

## Querying via CLI

You can use the Trino CLI to query data directly from the command line.

### Connect to Trino

```bash
trino-cli --server localhost:8080 --catalog iceberg --schema default
```

### Example Queries

Once connected to Trino, you can execute SQL queries:

```sql
-- Query customers table
SELECT * FROM customers LIMIT 10;

-- Query transactions table
SELECT * FROM transactions LIMIT 10;

-- Query products table
SELECT * FROM products LIMIT 10;

-- Join tables to get customer transactions with product details
SELECT 
  c.name as customer_name, 
  t.transaction_date, 
  t.amount, 
  p.name as product_name, 
  p.category
FROM 
  customers c 
  JOIN transactions t ON c.customer_id = t.customer_id 
  JOIN products p ON t.product_id = p.product_id 
LIMIT 10;

-- Get total sales by product category
SELECT 
  p.category, 
  SUM(t.amount) as total_sales
FROM 
  transactions t 
  JOIN products p ON t.product_id = p.product_id
GROUP BY 
  p.category
ORDER BY 
  total_sales DESC;

-- Get customers who spent more than $1000
SELECT 
  c.customer_id, 
  c.name, 
  SUM(t.amount) as total_spent
FROM 
  customers c 
  JOIN transactions t ON c.customer_id = t.customer_id
GROUP BY 
  c.customer_id, c.name
HAVING 
  SUM(t.amount) > 1000
ORDER BY 
  total_spent DESC;
```

## JDBC Connection

You can also connect to the Data Lakehouse using JDBC:

```java
String url = "jdbc:trino://localhost:8080/iceberg/default";
Connection connection = DriverManager.getConnection(url, "trino", null);

try (Statement statement = connection.createStatement();
     ResultSet resultSet = statement.executeQuery("SELECT * FROM customers LIMIT 10")) {
    
    while (resultSet.next()) {
        System.out.println(resultSet.getString("name"));
    }
}
```

## Troubleshooting

If you encounter issues when querying data:

1. Ensure the Data Lakehouse service is running
2. Verify that dummy data has been loaded
3. Check the service logs for any errors
4. Verify the connection parameters (host, port, catalog, schema)

For more information, refer to the [Trino documentation](https://trino.io/docs/current/) and the [Apache Iceberg documentation](https://iceberg.apache.org/).
