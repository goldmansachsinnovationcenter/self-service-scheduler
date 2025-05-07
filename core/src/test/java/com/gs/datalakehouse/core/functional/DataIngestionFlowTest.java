package com.gs.datalakehouse.core.functional;

import com.gs.datalakehouse.core.model.Column;
import com.gs.datalakehouse.core.model.DataSource;
import com.gs.datalakehouse.core.model.IngestionJob;
import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.DataSourceService;
import com.gs.datalakehouse.core.service.TableService;
import com.gs.datalakehouse.core.service.impl.DataSourceServiceImpl;
import com.gs.datalakehouse.core.service.impl.TableServiceImpl;
import com.gs.datalakehouse.core.util.DummyDataGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Functional test for the data ingestion flow, testing how different components
 * interact to create a complete data ingestion pipeline.
 */
public class DataIngestionFlowTest {

    private TableService tableService;
    private DataSourceService dataSourceService;
    private DummyDataGenerator dummyDataGenerator;

    @BeforeEach
    void setUp() {
        tableService = new TableServiceImpl();
        dataSourceService = new DataSourceServiceImpl();
        dummyDataGenerator = new DummyDataGenerator(tableService);
    }

    /**
     * Test the complete flow of setting up a data ingestion pipeline:
     * 1. Create a table
     * 2. Create a data source
     * 3. Create an ingestion job that connects the data source to the table
     */
    @Test
    void testCompleteDataIngestionFlow() {
        Table customerTable = createCustomerTable();
        Table createdTable = tableService.createTable(customerTable);
        
        assertNotNull(createdTable);
        assertEquals("customers", createdTable.getName());
        
        Optional<Table> retrievedTable = tableService.getTable("default", "customers");
        assertTrue(retrievedTable.isPresent());
        
        DataSource kafkaSource = createKafkaDataSource();
        DataSource createdSource = dataSourceService.createDataSource(kafkaSource);
        
        assertNotNull(createdSource);
        assertEquals("kafka-source", createdSource.getName());
        
        Optional<DataSource> retrievedSource = dataSourceService.getDataSource("kafka-source");
        assertTrue(retrievedSource.isPresent());
        
        IngestionJob ingestionJob = createIngestionJob(kafkaSource.getName(), customerTable.getDatabase() + "." + customerTable.getName());
        
        assertNotNull(ingestionJob);
        assertEquals("customer-ingestion", ingestionJob.getName());
        assertEquals(kafkaSource.getName(), ingestionJob.getSourceRef());
        assertEquals(customerTable.getDatabase() + "." + customerTable.getName(), ingestionJob.getTargetTableRef());
        
        assertEquals(1, tableService.listTables("default").size());
        assertEquals(1, dataSourceService.listDataSources().size());
    }
    
    /**
     * Test the flow with multiple tables and data sources
     */
    @Test
    void testMultipleTablesAndDataSources() {
        tableService.createTable(createCustomerTable());
        tableService.createTable(createProductTable());
        
        dataSourceService.createDataSource(createKafkaDataSource());
        dataSourceService.createDataSource(createFileDataSource());
        
        assertEquals(2, tableService.listTables("default").size());
        assertEquals(2, dataSourceService.listDataSources().size());
        
        IngestionJob customerKafkaJob = createIngestionJob("kafka-source", "default.customers");
        IngestionJob productFileJob = createIngestionJob("file-source", "default.products");
        
        assertNotNull(customerKafkaJob);
        assertNotNull(productFileJob);
        
        assertEquals("kafka-source", customerKafkaJob.getSourceRef());
        assertEquals("default.customers", customerKafkaJob.getTargetTableRef());
        assertEquals("file-source", productFileJob.getSourceRef());
        assertEquals("default.products", productFileJob.getTargetTableRef());
    }
    
    /**
     * Test updating tables and data sources in the flow
     */
    @Test
    void testUpdatingEntitiesInFlow() {
        Table customerTable = createCustomerTable();
        tableService.createTable(customerTable);
        
        DataSource kafkaSource = createKafkaDataSource();
        dataSourceService.createDataSource(kafkaSource);
        
        customerTable.setDescription("Updated customer table description");
        Table updatedTable = tableService.updateTable(customerTable);
        
        assertEquals("Updated customer table description", updatedTable.getDescription());
        
        Map<String, String> updatedProperties = new HashMap<>(kafkaSource.getProperties());
        updatedProperties.put("batch.size", "2000");
        kafkaSource.setProperties(updatedProperties);
        
        DataSource updatedSource = dataSourceService.updateDataSource(kafkaSource);
        assertEquals("2000", updatedSource.getProperties().get("batch.size"));
        
        Optional<Table> retrievedTable = tableService.getTable("default", "customers");
        assertTrue(retrievedTable.isPresent());
        assertEquals("Updated customer table description", retrievedTable.get().getDescription());
        
        Optional<DataSource> retrievedSource = dataSourceService.getDataSource("kafka-source");
        assertTrue(retrievedSource.isPresent());
        assertEquals("2000", retrievedSource.get().getProperties().get("batch.size"));
    }
    
    /**
     * Test deleting entities in the flow
     */
    @Test
    void testDeletingEntitiesInFlow() {
        tableService.createTable(createCustomerTable());
        tableService.createTable(createProductTable());
        dataSourceService.createDataSource(createKafkaDataSource());
        
        assertEquals(2, tableService.listTables("default").size());
        assertEquals(1, dataSourceService.listDataSources().size());
        
        boolean tableDeleted = tableService.deleteTable("default", "customers");
        assertTrue(tableDeleted);
        
        assertEquals(1, tableService.listTables("default").size());
        assertFalse(tableService.getTable("default", "customers").isPresent());
        assertTrue(tableService.getTable("default", "products").isPresent());
        
        boolean sourceDeleted = dataSourceService.deleteDataSource("kafka-source");
        assertTrue(sourceDeleted);
        
        assertEquals(0, dataSourceService.listDataSources().size());
        assertFalse(dataSourceService.getDataSource("kafka-source").isPresent());
    }
    
    
    private Table createCustomerTable() {
        Column idColumn = new Column("id", "string", false, "Customer ID");
        Column nameColumn = new Column("name", "string", false, "Customer name");
        Column emailColumn = new Column("email", "string", true, "Customer email");
        Column phoneColumn = new Column("phone", "string", true, "Customer phone");
        Column registrationDateColumn = new Column("registration_date", "date", false, "Registration date");
        Column statusColumn = new Column("status", "string", false, "Customer status");
        
        List<Column> columns = Arrays.asList(
            idColumn, nameColumn, emailColumn, phoneColumn, registrationDateColumn, statusColumn
        );
        
        List<String> partitionBy = Arrays.asList("registration_date");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.format.default", "parquet");
        
        return new Table(
            "customers",
            "default",
            "/warehouse/default/customers",
            columns,
            partitionBy,
            properties,
            "parquet",
            "Customer information table"
        );
    }
    
    private Table createProductTable() {
        Column idColumn = new Column("id", "string", false, "Product ID");
        Column nameColumn = new Column("name", "string", false, "Product name");
        Column priceColumn = new Column("price", "decimal(10,2)", false, "Product price");
        Column descriptionColumn = new Column("description", "string", true, "Product description");
        Column categoryColumn = new Column("category", "string", false, "Product category");
        Column createdAtColumn = new Column("created_at", "timestamp", false, "Creation timestamp");
        
        List<Column> columns = Arrays.asList(
            idColumn, nameColumn, priceColumn, descriptionColumn, categoryColumn, createdAtColumn
        );
        
        List<String> partitionBy = Arrays.asList("category");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.format.default", "parquet");
        
        return new Table(
            "products",
            "default",
            "/warehouse/default/products",
            columns,
            partitionBy,
            properties,
            "parquet",
            "Product catalog table"
        );
    }
    
    private DataSource createKafkaDataSource() {
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
    
    private DataSource createFileDataSource() {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "/data/files");
        properties.put("format", "csv");
        properties.put("header", "true");
        
        return new DataSource(
            "file-source",
            DataSource.SourceType.FILE,
            properties,
            "File data source for ingestion"
        );
    }
    
    private IngestionJob createIngestionJob(String sourceRef, String targetTableRef) {
        Map<String, String> properties = new HashMap<>();
        properties.put("batch.size", "1000");
        properties.put("checkpoint.location", "/checkpoints/customers");
        
        return new IngestionJob(
            "customer-ingestion",
            sourceRef,
            targetTableRef,
            "com.gs.datalakehouse.spark.job.KafkaToIcebergJob",
            properties,
            "0 */1 * * *",
            "Ingest customer data from Kafka to Iceberg"
        );
    }
}
