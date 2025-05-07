package com.gs.datalakehouse.core.util;

import com.gs.datalakehouse.core.model.DataSource;
import com.gs.datalakehouse.core.model.IngestionJob;
import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.TableService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DummyDataGeneratorTest {

    @Mock
    private TableService tableService;

    private DummyDataGenerator dummyDataGenerator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        dummyDataGenerator = new DummyDataGenerator(tableService);
    }

    @Test
    void testInitDummyData() {
        when(tableService.createTable(any(Table.class))).thenAnswer(invocation -> invocation.getArgument(0));
        
        dummyDataGenerator.initDummyData();
        
        verify(tableService, times(3)).createTable(any(Table.class));
    }

    @Test
    void testCreateCustomerTable() {
        when(tableService.createTable(any(Table.class))).thenAnswer(invocation -> {
            Table table = invocation.getArgument(0);
            assertEquals("customers", table.getName());
            assertEquals("default", table.getDatabase());
            assertEquals("/warehouse/default/customers", table.getLocation());
            assertEquals("parquet", table.getFormat());
            assertEquals("Customer information table", table.getDescription());
            assertEquals(6, table.getColumns().size());
            assertEquals(1, table.getPartitionBy().size());
            assertEquals("registration_date", table.getPartitionBy().get(0));
            return table;
        });
        
        try {
            java.lang.reflect.Method method = DummyDataGenerator.class.getDeclaredMethod("createCustomerTable");
            method.setAccessible(true);
            method.invoke(dummyDataGenerator);
        } catch (Exception e) {
            fail("Exception occurred while calling createCustomerTable: " + e.getMessage());
        }
        
        verify(tableService, times(1)).createTable(any(Table.class));
    }

    @Test
    void testCreateTransactionTable() {
        when(tableService.createTable(any(Table.class))).thenAnswer(invocation -> {
            Table table = invocation.getArgument(0);
            assertEquals("transactions", table.getName());
            assertEquals("default", table.getDatabase());
            assertEquals("/warehouse/default/transactions", table.getLocation());
            assertEquals("parquet", table.getFormat());
            assertEquals("Transaction records table", table.getDescription());
            assertEquals(6, table.getColumns().size());
            assertEquals(1, table.getPartitionBy().size());
            assertEquals("transaction_date", table.getPartitionBy().get(0));
            return table;
        });
        
        try {
            java.lang.reflect.Method method = DummyDataGenerator.class.getDeclaredMethod("createTransactionTable");
            method.setAccessible(true);
            method.invoke(dummyDataGenerator);
        } catch (Exception e) {
            fail("Exception occurred while calling createTransactionTable: " + e.getMessage());
        }
        
        verify(tableService, times(1)).createTable(any(Table.class));
    }

    @Test
    void testCreateProductTable() {
        when(tableService.createTable(any(Table.class))).thenAnswer(invocation -> {
            Table table = invocation.getArgument(0);
            assertEquals("products", table.getName());
            assertEquals("default", table.getDatabase());
            assertEquals("/warehouse/default/products", table.getLocation());
            assertEquals("parquet", table.getFormat());
            assertEquals("Product catalog table", table.getDescription());
            assertEquals(6, table.getColumns().size());
            assertEquals(1, table.getPartitionBy().size());
            assertEquals("category", table.getPartitionBy().get(0));
            return table;
        });
        
        try {
            java.lang.reflect.Method method = DummyDataGenerator.class.getDeclaredMethod("createProductTable");
            method.setAccessible(true);
            method.invoke(dummyDataGenerator);
        } catch (Exception e) {
            fail("Exception occurred while calling createProductTable: " + e.getMessage());
        }
        
        verify(tableService, times(1)).createTable(any(Table.class));
    }

    @Test
    void testCreateKafkaDataSource() {
        DataSource dataSource = dummyDataGenerator.createKafkaDataSource();
        
        assertNotNull(dataSource);
        assertEquals("kafka-source", dataSource.getName());
        assertEquals(DataSource.SourceType.KAFKA, dataSource.getType());
        assertEquals("Kafka data source for ingestion", dataSource.getDescription());
        assertEquals(3, dataSource.getProperties().size());
        assertEquals("localhost:9092", dataSource.getProperties().get("bootstrap.servers"));
        assertEquals("data-lakehouse-topic", dataSource.getProperties().get("topic"));
        assertEquals("data-lakehouse-group", dataSource.getProperties().get("group.id"));
    }

    @Test
    void testCreateCustomerIngestionJob() {
        IngestionJob job = dummyDataGenerator.createCustomerIngestionJob();
        
        assertNotNull(job);
        assertEquals("customer-ingestion", job.getName());
        assertEquals("kafka-source", job.getSourceRef());
        assertEquals("default.customers", job.getTargetTableRef());
        assertEquals("com.gs.datalakehouse.spark.job.KafkaToIcebergJob", job.getTransformationClass());
        assertEquals("0 */1 * * *", job.getSchedule());
        assertEquals("Ingest customer data from Kafka to Iceberg", job.getDescription());
        assertEquals(2, job.getProperties().size());
        assertEquals("1000", job.getProperties().get("batch.size"));
        assertEquals("/checkpoints/customers", job.getProperties().get("checkpoint.location"));
    }
}
