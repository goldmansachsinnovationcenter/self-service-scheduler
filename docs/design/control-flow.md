# Data Lakehouse Service Control Flow

## Overview

This document describes the control flow of the Data Lakehouse Service, illustrating how data moves through the system from ingestion to query. It covers the main data paths, component interactions, and processing stages.

## Data Ingestion Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │     │             │
│   Source    │────▶│    Kafka    │────▶│    Spark    │────▶│   Iceberg   │────▶│    HDFS     │
│   Systems   │     │   Brokers   │     │    Jobs     │     │   Tables    │     │   Storage   │
│             │     │             │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                   ▲                    │
                          │                   │                    │
                          ▼                   │                    ▼
                    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
                    │             │     │             │     │             │
                    │   Kafka     │     │  Metadata   │     │   Hadoop    │
                    │   Topics    │     │   Updates   │     │   Catalog   │
                    │             │     │             │     │             │
                    └─────────────┘     └─────────────┘     └─────────────┘
```

### Process Steps:

1. **Data Production**:
   - External systems produce data and send it to Kafka
   - Data is serialized in a compatible format (JSON, Avro, etc.)
   - Messages include metadata for processing

2. **Kafka Ingestion**:
   - Kafka receives messages and stores them in topics
   - Messages are persisted with configurable retention
   - Consumers can read messages at their own pace

3. **Spark Processing**:
   - Spark jobs consume data from Kafka topics
   - Data is validated, transformed, and enriched
   - Schema evolution is handled if needed
   - Data quality checks are performed

4. **Iceberg Table Writing**:
   - Processed data is written to Iceberg tables
   - Atomic transactions ensure data consistency
   - Metadata is updated in the Hadoop Catalog
   - Partitioning is applied based on configuration

5. **HDFS Storage**:
   - Parquet files are stored in HDFS
   - Files are organized by table and partition
   - Data is replicated for fault tolerance

## Query Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │
│    User     │────▶│  REST API   │────▶│    Trino    │────▶│   Iceberg   │
│  Interface  │     │  Endpoints  │     │   Engine    │     │   Tables    │
│             │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                                                                   │
                                                                   ▼
                                                           ┌─────────────┐
                                                           │             │
                                                           │    HDFS     │
                                                           │   Storage   │
                                                           │             │
                                                           └─────────────┘
```

### Process Steps:

1. **Query Submission**:
   - User submits SQL query through REST API
   - API validates the query syntax and permissions
   - Query is forwarded to Trino engine

2. **Query Planning**:
   - Trino parses and analyzes the SQL query
   - Query optimizer creates an execution plan
   - Metadata is retrieved from Hadoop Catalog
   - Data pruning is applied based on predicates

3. **Query Execution**:
   - Trino workers execute the query in parallel
   - Data is read from HDFS using Iceberg metadata
   - Predicate pushdown filters data at storage level
   - Intermediate results are processed in memory

4. **Result Delivery**:
   - Query results are collected and formatted
   - Results are returned to the API
   - API formats and returns results to the user

## Table Management Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │
│    User     │────▶│  REST API   │────▶│   Table     │────▶│   Iceberg   │
│  Interface  │     │  Endpoints  │     │  Service    │     │    API      │
│             │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                                                                   │
                                                                   ▼
                                                           ┌─────────────┐
                                                           │             │
                                                           │   Hadoop    │
                                                           │   Catalog   │
                                                           │             │
                                                           └─────────────┘
```

### Process Steps:

1. **Table Creation**:
   - User submits table definition through REST API
   - API validates the schema and configuration
   - TableService processes the request
   - Iceberg API creates the table in the catalog

2. **Schema Evolution**:
   - User submits schema change through REST API
   - API validates the compatibility of changes
   - TableService processes the request
   - Iceberg API updates the table schema

3. **Table Deletion**:
   - User submits deletion request through REST API
   - API validates permissions and dependencies
   - TableService processes the request
   - Iceberg API removes the table from the catalog

## Error Handling Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │
│  Component  │────▶│   Error     │────▶│   Logging   │
│   Failure   │     │  Handler    │     │   System    │
│             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
                          │
                          │
                          ▼
                    ┌─────────────┐     ┌─────────────┐
                    │             │     │             │
                    │   Retry     │────▶│   Error     │
                    │  Mechanism  │     │  Response   │
                    │             │     │             │
                    └─────────────┘     └─────────────┘
```

### Process Steps:

1. **Error Detection**:
   - Component detects an error condition
   - Error is captured with context information
   - Error handler is invoked

2. **Error Logging**:
   - Error details are logged with severity level
   - Stack trace and context are preserved
   - Metrics are updated for monitoring

3. **Error Recovery**:
   - Retry mechanism is applied if appropriate
   - Fallback strategies are executed if available
   - System state is restored to consistency

4. **Error Reporting**:
   - Appropriate error response is generated
   - User is notified with meaningful message
   - Monitoring systems are alerted if necessary

## Monitoring Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │
│  System     │────▶│   Metrics   │────▶│  Monitoring │────▶│  Alerting   │
│ Components  │     │ Collection  │     │   System    │     │   System    │
│             │     │             │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                              │
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │             │
                                        │ Dashboards  │
                                        │             │
                                        └─────────────┘
```

### Process Steps:

1. **Metrics Collection**:
   - Components emit metrics at regular intervals
   - Performance, health, and business metrics are collected
   - Metrics are tagged with component and environment information

2. **Metrics Processing**:
   - Monitoring system aggregates and processes metrics
   - Thresholds and anomaly detection are applied
   - Historical trends are analyzed

3. **Visualization**:
   - Dashboards display system health and performance
   - Key performance indicators are highlighted
   - Drill-down capabilities for troubleshooting

4. **Alerting**:
   - Alerts are triggered based on conditions
   - Notifications are sent to appropriate channels
   - Escalation policies are followed for critical issues
