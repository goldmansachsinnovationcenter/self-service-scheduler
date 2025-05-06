# Data Lakehouse Backend Service

A comprehensive data lakehouse backend service built on modern data technologies.

## Architecture

This service integrates the following components:
- **Apache Iceberg**: Table format for large analytic datasets
- **HDFS**: Distributed storage layer
- **Apache Spark**: Data processing and transformation engine
- **Trino**: SQL query engine for data access
- **Parquet**: Columnar storage format
- **Hadoop Catalog**: Metadata management
- **Kafka**: Source for streaming data

## Project Structure

- **Core Module**: Data models and service interfaces
- **Spark Module**: Kafka to Iceberg ingestion jobs
- **API Module**: REST endpoints for table management and queries

## Getting Started

See the documentation in the `docs` directory:
- [Installation Guide](docs/installation/installation.md)
- [Integration Guide](docs/integration/integration.md)
- [FAQ](docs/faq/faq.md)
- [Functional Testing](docs/testing/functional-testing.md)

## Running the Service

Use the provided scripts in the `scripts` directory:
```bash
# Set up the environment
./scripts/setup-environment.sh

# Start the API service
./scripts/start-api.sh

# Submit a Spark job for Kafka ingestion
./scripts/start-spark-job.sh <kafka-brokers> <topic> <catalog> <table>
```
