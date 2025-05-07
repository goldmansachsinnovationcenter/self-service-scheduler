# Data Lakehouse Service Data Model

## Overview

This document describes the data model used in the Data Lakehouse Service. The data model defines the structure of the core entities in the system and their relationships.

## Core Entities

### Table

The `Table` entity represents a data table in the lakehouse. Tables are the primary data storage units in the system.

**Attributes:**
- `id`: Unique identifier for the table
- `name`: Name of the table
- `description`: Description of the table's purpose and contents
- `format`: Storage format (e.g., Parquet, Avro)
- `location`: Physical storage location
- `partitionBy`: List of columns used for partitioning
- `properties`: Additional table properties
- `columns`: List of columns in the table
- `createdAt`: Timestamp when the table was created
- `updatedAt`: Timestamp when the table was last updated

### Column

The `Column` entity represents a column in a table.

**Attributes:**
- `id`: Unique identifier for the column
- `name`: Name of the column
- `type`: Data type of the column
- `description`: Description of the column's purpose and contents
- `nullable`: Whether the column can contain null values
- `defaultValue`: Default value for the column
- `comment`: Additional comments about the column
- `order`: Position of the column in the table

### DataSource

The `DataSource` entity represents a source of data that can be ingested into the lakehouse.

**Attributes:**
- `id`: Unique identifier for the data source
- `name`: Name of the data source
- `type`: Type of data source (e.g., Kafka, JDBC, File)
- `connectionProperties`: Properties required to connect to the data source
- `format`: Format of the data in the source
- `schema`: Schema of the data in the source
- `createdAt`: Timestamp when the data source was created
- `updatedAt`: Timestamp when the data source was last updated

### IngestionJob

The `IngestionJob` entity represents a job that ingests data from a data source into a table.

**Attributes:**
- `id`: Unique identifier for the ingestion job
- `name`: Name of the ingestion job
- `description`: Description of the job's purpose
- `sourceId`: Reference to the data source
- `targetTableId`: Reference to the target table
- `schedule`: Schedule for the job (cron expression)
- `transformations`: Data transformations to apply during ingestion
- `status`: Current status of the job
- `lastRunTime`: Timestamp of the last execution
- `lastRunStatus`: Status of the last execution
- `createdAt`: Timestamp when the job was created
- `updatedAt`: Timestamp when the job was last updated

## Relationships

The following diagram illustrates the relationships between the core entities:

```
DataSource 1 --- * IngestionJob * --- 1 Table 1 --- * Column
```

- A DataSource can be used by multiple IngestionJobs
- An IngestionJob ingests data from one DataSource into one Table
- A Table contains multiple Columns

## Schema Evolution

The Data Lakehouse Service supports schema evolution through Apache Iceberg. This allows for:

1. **Adding Columns**: New columns can be added to existing tables without affecting existing data
2. **Renaming Columns**: Columns can be renamed without changing the underlying data
3. **Widening Types**: Column types can be widened (e.g., int to long) safely
4. **Type Conversion**: Some type conversions are supported with explicit configuration

## Metadata Management

Table and schema metadata are managed through:

1. **Iceberg Metadata Tables**: Track table history, snapshots, and data files
2. **System Catalog**: Maintains information about all tables, columns, and data sources
3. **Job Registry**: Tracks all ingestion jobs and their execution history

## Data Governance

The data model supports governance through:

1. **Column-level Lineage**: Tracking the origin and transformations of each column
2. **Access Control**: Fine-grained permissions at the table and column level
3. **Audit Logging**: Recording all changes to the data model and data access patterns
