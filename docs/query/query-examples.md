# Querying Data in the Data Lakehouse

This document provides examples of how to query the dummy data in the Data Lakehouse using different interfaces: API, CLI, and Swagger UI.

## Sample Data Overview

The Data Lakehouse contains the following sample tables:

1. **customers** - Customer information
2. **products** - Product catalog
3. **transactions** - Transaction records

## Querying via REST API

The Data Lakehouse provides a REST API for executing SQL queries against the data.

### API Endpoint

```
POST /api/query
```

### Request Format

```json
{
  "sql": "SELECT * FROM customers LIMIT 10"
}
```

### Example Queries

#### 1. List all customers

```bash
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM customers"}'
```

#### 2. Get total sales by product

```bash
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT p.name, SUM(t.amount) as total_sales FROM transactions t JOIN products p ON t.product_id = p.product_id GROUP BY p.name ORDER BY total_sales DESC"}'
```

#### 3. Find customers who made purchases in the last 7 days

```bash
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT c.customer_id, c.name, c.email FROM customers c JOIN transactions t ON c.customer_id = t.customer_id WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '\''7'\'' DAY GROUP BY c.customer_id, c.name, c.email"}'
```

## Querying via CLI

You can use the Trino CLI to query the data directly. The Trino CLI is a command-line interface for executing SQL queries against Trino.

### Prerequisites

1. Download the Trino CLI:
   ```bash
   wget https://repo1.maven.org/maven2/io/trino/trino-cli/412/trino-cli-412-executable.jar -O trino
   chmod +x trino
   ```

2. Connect to the Trino server:
   ```bash
   ./trino --server localhost:8080 --catalog iceberg --schema default
   ```

### Example Queries

Once connected to the Trino CLI, you can execute SQL queries:

#### 1. List all customers

```sql
SELECT * FROM customers;
```

#### 2. Get total sales by product

```sql
SELECT p.name, SUM(t.amount) as total_sales 
FROM transactions t 
JOIN products p ON t.product_id = p.product_id 
GROUP BY p.name 
ORDER BY total_sales DESC;
```

#### 3. Find customers who made purchases in the last 7 days

```sql
SELECT c.customer_id, c.name, c.email 
FROM customers c 
JOIN transactions t ON c.customer_id = t.customer_id 
WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '7' DAY 
GROUP BY c.customer_id, c.name, c.email;
```

## Querying via Swagger UI

The Data Lakehouse provides a Swagger UI for executing queries through a web interface.

### Accessing Swagger UI

1. Open a web browser and navigate to:
   ```
   http://localhost:8080/swagger-ui.html
   ```

2. Find the `/api/query` endpoint in the Query Controller section.

### Using Swagger UI for Queries

1. Click on the `/api/query` POST endpoint to expand it.
2. Click on the "Try it out" button.
3. In the Request Body field, enter your SQL query in JSON format:
   ```json
   {
     "sql": "SELECT * FROM customers LIMIT 10"
   }
   ```
4. Click on the "Execute" button to run the query.
5. The response will be displayed in the "Response body" section.

### Example Queries for Swagger UI

#### 1. List all customers

```json
{
  "sql": "SELECT * FROM customers"
}
```

#### 2. Get total sales by product

```json
{
  "sql": "SELECT p.name, SUM(t.amount) as total_sales FROM transactions t JOIN products p ON t.product_id = p.product_id GROUP BY p.name ORDER BY total_sales DESC"
}
```

#### 3. Find customers who made purchases in the last 7 days

```json
{
  "sql": "SELECT c.customer_id, c.name, c.email FROM customers c JOIN transactions t ON c.customer_id = t.customer_id WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '7' DAY GROUP BY c.customer_id, c.name, c.email"
}
```

## Additional Query Examples

### 1. Find the most popular product category

```sql
SELECT p.category, COUNT(*) as purchase_count 
FROM transactions t 
JOIN products p ON t.product_id = p.product_id 
GROUP BY p.category 
ORDER BY purchase_count DESC 
LIMIT 1;
```

### 2. Calculate average transaction amount by payment method

```sql
SELECT payment_method, AVG(amount) as avg_amount 
FROM transactions 
GROUP BY payment_method 
ORDER BY avg_amount DESC;
```

### 3. Find customers who have made multiple purchases

```sql
SELECT c.customer_id, c.name, COUNT(*) as purchase_count 
FROM customers c 
JOIN transactions t ON c.customer_id = t.customer_id 
GROUP BY c.customer_id, c.name 
HAVING COUNT(*) > 1 
ORDER BY purchase_count DESC;
```

### 4. Get inventory status for products with low stock

```sql
SELECT product_id, name, inventory 
FROM products 
WHERE inventory < 30 
ORDER BY inventory ASC;
```

### 5. Find transactions with amounts above average

```sql
SELECT t.transaction_id, c.name as customer_name, p.name as product_name, t.amount 
FROM transactions t 
JOIN customers c ON t.customer_id = c.customer_id 
JOIN products p ON t.product_id = p.product_id 
WHERE t.amount > (SELECT AVG(amount) FROM transactions) 
ORDER BY t.amount DESC;
```
