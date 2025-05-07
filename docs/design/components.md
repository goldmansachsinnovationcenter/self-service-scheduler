# Data Lakehouse Service Component Design

## Overview

This document describes the key components of the Data Lakehouse Service and their interactions. The service is built on a modular architecture with clear separation of concerns between components.

## Component Architecture

The Data Lakehouse Service is organized into three main modules:

1. **Core Module**
2. **API Module**
3. **Spark Module**

Each module has specific responsibilities and interfaces with other modules through well-defined APIs.

## Core Module

The Core Module serves as the foundation of the system, providing domain models and service interfaces.

### Key Components

#### Domain Models

- **Table**: Represents a data table in the lakehouse
- **Column**: Represents a column in a table
- **DataSource**: Represents a source of data for ingestion
- **IngestionJob**: Represents a job that ingests data from a source to a table

#### Service Interfaces

- **TableService**: Manages table operations (create, read, update, delete)
- **DataSourceService**: Manages data source operations
- **IngestionService**: Manages ingestion job operations

#### Service Implementations

- **TableServiceImpl**: Implements TableService using Iceberg APIs
- **DataSourceServiceImpl**: Implements DataSourceService
- **IngestionServiceImpl**: Implements IngestionService

## API Module

The API Module exposes REST endpoints for interacting with the system.

### Key Components

#### Controllers

- **TableController**: Exposes endpoints for table operations
- **QueryController**: Exposes endpoints for executing queries

#### Configuration

- **OpenApiConfig**: Configures Swagger/OpenAPI documentation
- **SecurityConfig**: Configures authentication and authorization
- **CorsConfig**: Configures Cross-Origin Resource Sharing

#### DTOs (Data Transfer Objects)

- **TableDTO**: Represents a table in API responses
- **ColumnDTO**: Represents a column in API responses
- **QueryRequestDTO**: Represents a query request
- **QueryResponseDTO**: Represents a query response

## Spark Module

The Spark Module implements data processing jobs using Apache Spark.

### Key Components

#### Jobs

- **KafkaToIcebergJob**: Ingests data from Kafka to Iceberg tables
- **DummyDataGenerator**: Generates test data for development and testing

#### Utilities

- **SparkSessionFactory**: Creates and configures Spark sessions
- **SchemaConverter**: Converts between Spark and Iceberg schemas
- **TransformationUtils**: Utilities for data transformation

## Component Interactions

### Data Ingestion Flow

1. **External System** produces data to Kafka topics
2. **KafkaToIcebergJob** in the Spark Module consumes data from Kafka
3. **KafkaToIcebergJob** uses the Core Module's domain models to represent the data
4. **KafkaToIcebergJob** writes data to Iceberg tables using Iceberg APIs

### Data Query Flow

1. **Client** sends a query request to the API Module
2. **QueryController** in the API Module receives the request
3. **QueryController** uses the Core Module's services to execute the query
4. **QueryController** returns the query results to the client

### Table Management Flow

1. **Client** sends a table management request to the API Module
2. **TableController** in the API Module receives the request
3. **TableController** uses the Core Module's TableService to perform the operation
4. **TableController** returns the result to the client

## Extension Points

The component design includes several extension points for future enhancements:

1. **Additional Data Sources**: New data source types can be added by implementing the DataSource interface
2. **Custom Transformations**: New data transformations can be added to the Spark Module
3. **Additional Query Engines**: Support for additional query engines can be added alongside Trino
4. **Authentication Providers**: New authentication providers can be integrated through the SecurityConfig

## Deployment Considerations

The components can be deployed in various configurations:

1. **Monolithic Deployment**: All components deployed as a single application
2. **Microservices Deployment**: Components deployed as separate microservices
3. **Hybrid Deployment**: Core and API modules deployed together, with Spark jobs deployed separately

Each deployment model has trade-offs in terms of complexity, scalability, and resource utilization.
