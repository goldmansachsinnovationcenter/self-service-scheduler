package com.gs.datalakehouse.core.util;

import com.gs.datalakehouse.core.model.Column;
import com.gs.datalakehouse.core.model.DataSource;
import com.gs.datalakehouse.core.model.IngestionJob;
import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to generate dummy data for the data lakehouse service.
 */
@Component
public class DummyDataGenerator {

    private final TableService tableService;

    @Autowired
    public DummyDataGenerator(TableService tableService) {
        this.tableService = tableService;
    }

    /**
     * Initializes the data lakehouse with dummy data.
     */
    @PostConstruct
    public void initDummyData() {
        createCustomerTable();
        
        createTransactionTable();
        
        createProductTable();
        
        System.out.println("Dummy data initialized successfully.");
    }

    /**
     * Creates a customer table with sample data.
     */
    private void createCustomerTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("customer_id", "string", false, "Primary key"));
        columns.add(new Column("name", "string", false, "Customer name"));
        columns.add(new Column("email", "string", true, "Customer email"));
        columns.add(new Column("phone", "string", true, "Customer phone number"));
        columns.add(new Column("address", "string", true, "Customer address"));
        columns.add(new Column("registration_date", "timestamp", false, "Registration date"));
        
        List<String> partitionBy = Arrays.asList("registration_date");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.format.default", "parquet");
        
        Table customerTable = new Table(
            "customers",
            "default",
            "/warehouse/default/customers",
            columns,
            partitionBy,
            properties,
            "parquet",
            "Customer information table"
        );
        
        tableService.createTable(customerTable);
    }

    /**
     * Creates a transaction table with sample data.
     */
    private void createTransactionTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("transaction_id", "string", false, "Primary key"));
        columns.add(new Column("customer_id", "string", false, "Customer ID"));
        columns.add(new Column("product_id", "string", false, "Product ID"));
        columns.add(new Column("transaction_date", "timestamp", false, "Transaction date"));
        columns.add(new Column("amount", "double", false, "Transaction amount"));
        columns.add(new Column("payment_method", "string", true, "Payment method"));
        
        List<String> partitionBy = Arrays.asList("transaction_date");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.format.default", "parquet");
        
        Table transactionTable = new Table(
            "transactions",
            "default",
            "/warehouse/default/transactions",
            columns,
            partitionBy,
            properties,
            "parquet",
            "Transaction records table"
        );
        
        tableService.createTable(transactionTable);
    }

    /**
     * Creates a product table with sample data.
     */
    private void createProductTable() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("product_id", "string", false, "Primary key"));
        columns.add(new Column("name", "string", false, "Product name"));
        columns.add(new Column("category", "string", true, "Product category"));
        columns.add(new Column("price", "double", false, "Product price"));
        columns.add(new Column("inventory", "integer", true, "Available inventory"));
        columns.add(new Column("last_updated", "timestamp", false, "Last update timestamp"));
        
        List<String> partitionBy = Arrays.asList("category");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.format.default", "parquet");
        
        Table productTable = new Table(
            "products",
            "default",
            "/warehouse/default/products",
            columns,
            partitionBy,
            properties,
            "parquet",
            "Product catalog table"
        );
        
        tableService.createTable(productTable);
    }

    /**
     * Creates a Kafka data source.
     */
    public DataSource createKafkaDataSource() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("topic", "data-lakehouse-topic");
        properties.put("group.id", "data-lakehouse-group");
        
        return new DataSource(
            "kafka-source",
            DataSource.SourceType.KAFKA,
            properties,
            "Kafka data source for ingestion"
        );
    }

    /**
     * Creates an ingestion job for customer data.
     */
    public IngestionJob createCustomerIngestionJob() {
        Map<String, String> properties = new HashMap<>();
        properties.put("batch.size", "1000");
        properties.put("checkpoint.location", "/checkpoints/customers");
        
        return new IngestionJob(
            "customer-ingestion",
            "kafka-source",
            "default.customers",
            "com.gs.datalakehouse.spark.job.KafkaToIcebergJob",
            properties,
            "0 */1 * * *", // Every hour
            "Ingest customer data from Kafka to Iceberg"
        );
    }
}
