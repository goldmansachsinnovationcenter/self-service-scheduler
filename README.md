# Data Lakehouse Backend Service

A comprehensive data lakehouse backend service built on modern data technologies, providing a unified platform for data storage, processing, and analytics.

## Overview

The Data Lakehouse Service combines the best features of data warehouses and data lakes, offering:

- **ACID Transactions**: Ensures data consistency and reliability
- **Schema Evolution**: Supports changing data structures over time
- **Time Travel**: Access historical versions of data
- **High Performance**: Optimized for both batch and interactive queries
- **Open Format**: Based on open standards and file formats
- **Scalability**: Horizontally scalable architecture
- **Streaming Support**: Real-time data ingestion from Kafka

## Architecture

This service integrates the following components:

- **Apache Iceberg**: Table format for large analytic datasets with ACID transactions
- **HDFS**: Distributed storage layer for scalable and reliable data storage
- **Apache Spark**: Data processing and transformation engine for batch and streaming
- **Trino**: SQL query engine for high-performance data access and analytics
- **Parquet**: Columnar storage format for efficient data storage and retrieval
- **Hadoop Catalog**: Metadata management for table schemas and partitions
- **Kafka**: Source for streaming data with real-time ingestion capabilities

For detailed architecture diagrams and component interactions, see the [Architecture Documentation](docs/design/architecture.md) and [Control Flow Documentation](docs/design/control-flow.md).

## Project Structure

The project is organized into modular components:

- **Core Module**: Data models, service interfaces, and common utilities
  - Table and column definitions
  - Service interfaces for data management
  - Utility classes for common operations

- **Spark Module**: Kafka to Iceberg ingestion jobs
  - Streaming data processing
  - Batch data processing
  - Data transformation and validation

- **API Module**: REST endpoints for table management and queries
  - Table creation, modification, and deletion
  - SQL query execution
  - Swagger UI for API documentation and testing

## Features

- **Data Ingestion**: Stream data from Kafka topics into Iceberg tables
- **Data Transformation**: Process and transform data using Apache Spark
- **Data Storage**: Store data efficiently in Parquet format on HDFS
- **Data Querying**: Execute SQL queries using Trino engine
- **Table Management**: Create, modify, and delete tables via REST API
- **Schema Evolution**: Update table schemas without data migration
- **Time Travel**: Query historical versions of data
- **Metadata Management**: Track table schemas and partitions
- **API Documentation**: Explore and test API endpoints using Swagger UI

## Getting Started

See the documentation in the `docs` directory:

- [Installation Guide](docs/installation/installation.md): Set up the service
- [Integration Guide](docs/integration/integration.md): Integrate with other systems
- [Query Instructions](docs/query/query-instructions.md): Learn how to query data
- [FAQ](docs/faq/faq.md): Common questions and answers
- [Functional Testing](docs/testing/functional-testing.md): Test the service functionality
- [Design Documentation](docs/design/): Architecture and control flow diagrams

## Running the Service

Use the provided scripts in the `scripts` directory:

```bash
# Set up the environment (HDFS, Kafka, Trino)
./scripts/setup-environment.sh

# Load dummy data for testing
./scripts/load-dummy-data.sh

# Start the API service
./scripts/start-api.sh

# Launch the complete service with all components
./scripts/launch-service.sh

# Submit a Spark job for Kafka ingestion
./scripts/start-spark-job.sh <kafka-brokers> <topic> <catalog> <table>
```

## API Access

Once the service is running:

- **REST API**: Access the API at `http://localhost:8081/api`
- **Swagger UI**: Explore the API at `http://localhost:8081/swagger-ui.html`
- **Trino CLI**: Connect using `trino-cli --server localhost:8080 --catalog iceberg --schema default`
- **JDBC**: Connect using JDBC URL `jdbc:trino://localhost:8080/iceberg/default`

## Development

- Java 11 is required for development
- Maven is used for dependency management and building
- JUnit 5 and Mockito are used for testing
- JaCoCo is used for code coverage reporting

## License

Proprietary - Goldman Sachs Innovation Center
