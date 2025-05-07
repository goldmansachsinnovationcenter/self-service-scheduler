package com.gs.datalakehouse.core.acceptance;

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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Acceptance tests for the Data Lakehouse Service.
 * These tests verify that the system meets the requirements from a user's perspective.
 */
public class DataLakehouseAcceptanceTest {

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
     * Acceptance test for user story: "As a data engineer, I want to create a table in the data lakehouse
     * so that I can store structured data."
     */
    @Test
    @DisplayName("User can create a table in the data lakehouse")
    void testUserCanCreateTable() {
        Column idColumn = new Column("id", "string", false, "Primary key");
        Column nameColumn = new Column("name", "string", false, "Customer name");
        List<Column> columns = Arrays.asList(idColumn, nameColumn);
        
        Table table = new Table();
        table.setName("customers");
        table.setDatabase("default");
        table.setColumns(columns);
        table.setFormat("parquet");
        table.setDescription("Customer table");
        
        Table createdTable = tableService.createTable(table);
        
        assertNotNull(createdTable);
        assertEquals("customers", createdTable.getName());
        
        Optional<Table> retrievedTable = tableService.getTable("default", "customers");
        assertTrue(retrievedTable.isPresent());
        assertEquals("customers", retrievedTable.get().getName());
        assertEquals(2, retrievedTable.get().getColumns().size());
    }

    /**
     * Acceptance test for user story: "As a data engineer, I want to configure a data source
     * so that I can ingest data from external systems."
     */
    @Test
    @DisplayName("User can configure a data source")
    void testUserCanConfigureDataSource() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "kafka:9092");
        properties.put("topic", "users");
        
        DataSource dataSource = new DataSource();
        dataSource.setName("kafka-users");
        dataSource.setType(DataSource.SourceType.KAFKA);
        dataSource.setProperties(properties);
        dataSource.setDescription("Kafka source for user data");
        
        DataSource createdSource = dataSourceService.createDataSource(dataSource);
        
        assertNotNull(createdSource);
        assertEquals("kafka-users", createdSource.getName());
        
        Optional<DataSource> retrievedSource = dataSourceService.getDataSource("kafka-users");
        assertTrue(retrievedSource.isPresent());
        assertEquals("kafka-users", retrievedSource.get().getName());
        assertEquals(DataSource.SourceType.KAFKA, retrievedSource.get().getType());
        assertEquals("kafka:9092", retrievedSource.get().getProperties().get("bootstrap.servers"));
    }

    /**
     * Acceptance test for user story: "As a data engineer, I want to set up an ingestion job
     * so that data is automatically loaded from a source to a table."
     */
    @Test
    @DisplayName("User can set up an ingestion job")
    void testUserCanSetUpIngestionJob() {
        Table table = new Table();
        table.setName("users");
        table.setDatabase("default");
        table.setFormat("parquet");
        tableService.createTable(table);
        
        DataSource dataSource = new DataSource();
        dataSource.setName("kafka-users");
        dataSource.setType(DataSource.SourceType.KAFKA);
        dataSourceService.createDataSource(dataSource);
        
        IngestionJob job = new IngestionJob();
        job.setName("user-ingestion");
        job.setSourceRef("kafka-users");
        job.setTargetTableRef("default.users");
        job.setTransformationClass("com.gs.datalakehouse.spark.job.KafkaToIcebergJob");
        job.setSchedule("0 */1 * * *");
        
        assertNotNull(job);
        assertEquals("user-ingestion", job.getName());
        assertEquals("kafka-users", job.getSourceRef());
        assertEquals("default.users", job.getTargetTableRef());
        assertEquals("0 */1 * * *", job.getSchedule());
        
        assertTrue(tableService.getTable("default", "users").isPresent());
        assertTrue(dataSourceService.getDataSource("kafka-users").isPresent());
    }

    /**
     * Acceptance test for user story: "As a data analyst, I want to query tables in the data lakehouse
     * so that I can analyze the data."
     */
    @Test
    @DisplayName("User can query tables in the data lakehouse")
    void testUserCanQueryTables() {
        dummyDataGenerator.initDummyData();
        
        List<Table> tables = tableService.listTables("default");
        
        assertFalse(tables.isEmpty());
        
        Optional<Table> customersTable = tables.stream()
            .filter(t -> t.getName().equals("customers"))
            .findFirst();
            
        assertTrue(customersTable.isPresent());
        assertNotNull(customersTable.get().getColumns());
        assertFalse(customersTable.get().getColumns().isEmpty());
        
        List<Column> columns = customersTable.get().getColumns();
        assertTrue(columns.stream().anyMatch(c -> c.getName().equals("id")));
        assertTrue(columns.stream().anyMatch(c -> c.getName().equals("name")));
    }

    /**
     * Acceptance test for user story: "As a data engineer, I want to update table schemas
     * so that I can adapt to changing data requirements."
     */
    @Test
    @DisplayName("User can update table schemas")
    void testUserCanUpdateTableSchemas() {
        Column idColumn = new Column("id", "string", false, "Primary key");
        Column nameColumn = new Column("name", "string", false, "Customer name");
        List<Column> columns = Arrays.asList(idColumn, nameColumn);
        
        Table table = new Table();
        table.setName("customers");
        table.setDatabase("default");
        table.setColumns(columns);
        table.setFormat("parquet");
        
        tableService.createTable(table);
        
        Column emailColumn = new Column("email", "string", true, "Customer email");
        List<Column> updatedColumns = Arrays.asList(idColumn, nameColumn, emailColumn);
        table.setColumns(updatedColumns);
        
        Table updatedTable = tableService.updateTable(table);
        
        assertNotNull(updatedTable);
        assertEquals(3, updatedTable.getColumns().size());
        
        Optional<Table> retrievedTable = tableService.getTable("default", "customers");
        assertTrue(retrievedTable.isPresent());
        assertEquals(3, retrievedTable.get().getColumns().size());
        assertTrue(retrievedTable.get().getColumns().stream()
            .anyMatch(c -> c.getName().equals("email")));
    }

    /**
     * Acceptance test for user story: "As a data steward, I want to delete obsolete tables
     * so that I can maintain a clean data lakehouse."
     */
    @Test
    @DisplayName("User can delete obsolete tables")
    void testUserCanDeleteObsoleteTables() {
        Table table1 = new Table();
        table1.setName("active_table");
        table1.setDatabase("default");
        
        Table table2 = new Table();
        table2.setName("obsolete_table");
        table2.setDatabase("default");
        
        tableService.createTable(table1);
        tableService.createTable(table2);
        
        boolean deleted = tableService.deleteTable("default", "obsolete_table");
        
        assertTrue(deleted);
        
        Optional<Table> retrievedTable = tableService.getTable("default", "obsolete_table");
        assertFalse(retrievedTable.isPresent());
        
        List<Table> remainingTables = tableService.listTables("default");
        assertEquals(1, remainingTables.size());
        assertEquals("active_table", remainingTables.get(0).getName());
    }
}
