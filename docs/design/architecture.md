# Data Lakehouse Service Architecture

## Overview

The Self-Service Scheduler is a comprehensive data lakehouse backend service that integrates multiple modern data technologies to provide a robust platform for data storage, processing, and analysis. This document explains the architecture and how the various components interact within the ecosystem.

## System Architecture

![Data Lakehouse Architecture](../images/architecture.png)

The Data Lakehouse Service is built on a modular architecture with the following key components:

### Core Components

1. **Core Module**
   - Contains the domain models and service interfaces
   - Defines the data structures for tables, columns, data sources, and ingestion jobs
   - Provides service implementations for data management operations

2. **API Module**
   - Exposes REST endpoints for interacting with the system
   - Implements controllers for table and query operations
   - Provides Swagger/OpenAPI documentation for API exploration
   - Built on Spring Boot for robust web service capabilities

3. **Spark Module**
   - Implements data processing jobs using Apache Spark
   - Handles Kafka to Iceberg data ingestion
   - Provides utilities for data transformation and enrichment

### Technology Stack

The service integrates the following technologies:

1. **Apache Iceberg**
   - Table format for large analytic datasets
   - Provides schema evolution, hidden partitioning, and snapshot isolation
   - Enables time travel queries and rollback capabilities

2. **HDFS (Hadoop Distributed File System)**
   - Distributed storage layer for data files
   - Provides redundancy and high availability for data storage
   - Scales horizontally to accommodate growing data volumes

3. **Apache Spark**
   - Data processing and transformation engine
   - Executes batch and streaming workloads
   - Integrates with Iceberg for efficient data writing and reading

4. **Trino (formerly PrestoSQL)**
   - SQL query engine for data access
   - Enables federated queries across multiple data sources
   - Provides interactive query capabilities for data analysis

5. **Apache Parquet**
   - Columnar storage format for efficient data storage and retrieval
   - Reduces I/O operations and improves query performance
   - Supports predicate pushdown for optimized filtering

6. **Hadoop Catalog**
   - Metadata management for Iceberg tables
   - Tracks table schemas, partitioning, and data file locations
   - Enables consistent metadata operations across distributed systems

7. **Apache Kafka**
   - Source for streaming data
   - Provides durable message storage and reliable data delivery
   - Enables real-time data ingestion pipelines

## Data Flow

The typical data flow through the system follows these steps:

1. **Data Ingestion**
   - Data is produced to Kafka topics by external systems
   - Spark streaming jobs consume data from Kafka
   - Data is validated, transformed, and written to Iceberg tables

2. **Data Storage**
   - Iceberg manages table metadata and schema evolution
   - Data files are stored in Parquet format on HDFS
   - Snapshots provide point-in-time views of the data

3. **Data Access**
   - REST API provides programmatic access to data
   - Trino enables SQL-based querying of Iceberg tables
   - Spark can be used for complex analytical processing

## Security Model

The service implements a comprehensive security model:

1. **Authentication**
   - API endpoints require authentication
   - Integration with enterprise identity providers

2. **Authorization**
   - Role-based access control for data operations
   - Fine-grained permissions at the table and column level

3. **Data Protection**
   - Encryption for data at rest and in transit
   - Audit logging for all data access and modifications

## Deployment Model

The service can be deployed in various environments:

1. **On-Premises**
   - Deployed on enterprise Hadoop clusters
   - Integrated with existing data infrastructure

2. **Cloud-Based**
   - Deployable on AWS, Azure, or GCP
   - Leverages cloud-native storage (S3, ADLS, GCS)

3. **Hybrid**
   - Spans on-premises and cloud environments
   - Enables data federation across deployment boundaries

## Monitoring and Management

The service provides comprehensive monitoring and management capabilities:

1. **Metrics Collection**
   - JVM and application-level metrics
   - Data ingestion and query performance statistics

2. **Logging**
   - Structured logging for all system components
   - Centralized log aggregation and analysis

3. **Alerting**
   - Threshold-based alerts for system health
   - Anomaly detection for data quality issues

## Conclusion

The Data Lakehouse Service provides a modern, scalable platform for enterprise data management. By integrating best-of-breed technologies like Apache Iceberg, Spark, and Trino, it delivers a comprehensive solution for data storage, processing, and analysis.
