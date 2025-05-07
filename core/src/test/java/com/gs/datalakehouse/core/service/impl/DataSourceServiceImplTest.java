package com.gs.datalakehouse.core.service.impl;

import com.gs.datalakehouse.core.model.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DataSourceServiceImplTest {

    private DataSourceServiceImpl dataSourceService;

    @BeforeEach
    void setUp() {
        dataSourceService = new DataSourceServiceImpl();
    }

    @Test
    void testCreateDataSource() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("topic", "test-topic");
        
        DataSource dataSource = new DataSource(
            "kafka-source",
            DataSource.SourceType.KAFKA,
            properties,
            "Kafka data source for testing"
        );
        
        DataSource createdDataSource = dataSourceService.createDataSource(dataSource);
        
        assertNotNull(createdDataSource);
        assertEquals("kafka-source", createdDataSource.getName());
        assertEquals(DataSource.SourceType.KAFKA, createdDataSource.getType());
        assertEquals(2, createdDataSource.getProperties().size());
        assertEquals("localhost:9092", createdDataSource.getProperties().get("bootstrap.servers"));
        assertEquals("test-topic", createdDataSource.getProperties().get("topic"));
        assertEquals("Kafka data source for testing", createdDataSource.getDescription());
        
        Optional<DataSource> retrievedDataSource = dataSourceService.getDataSource("kafka-source");
        assertTrue(retrievedDataSource.isPresent());
        assertEquals("kafka-source", retrievedDataSource.get().getName());
    }
    
    @Test
    void testGetDataSource() {
        DataSource dataSource = new DataSource();
        dataSource.setName("jdbc-source");
        dataSource.setType(DataSource.SourceType.DATABASE);
        dataSource.setDescription("JDBC data source for testing");
        
        dataSourceService.createDataSource(dataSource);
        
        Optional<DataSource> retrievedDataSource = dataSourceService.getDataSource("jdbc-source");
        
        assertTrue(retrievedDataSource.isPresent());
        assertEquals("jdbc-source", retrievedDataSource.get().getName());
        assertEquals(DataSource.SourceType.DATABASE, retrievedDataSource.get().getType());
        assertEquals("JDBC data source for testing", retrievedDataSource.get().getDescription());
        
        Optional<DataSource> nonExistentDataSource = dataSourceService.getDataSource("nonexistent");
        assertFalse(nonExistentDataSource.isPresent());
    }
    
    @Test
    void testListDataSources() {
        DataSource dataSource1 = new DataSource();
        dataSource1.setName("source1");
        dataSource1.setType(DataSource.SourceType.KAFKA);
        
        DataSource dataSource2 = new DataSource();
        dataSource2.setName("source2");
        dataSource2.setType(DataSource.SourceType.FILE);
        
        dataSourceService.createDataSource(dataSource1);
        dataSourceService.createDataSource(dataSource2);
        
        List<DataSource> dataSources = dataSourceService.listDataSources();
        
        assertNotNull(dataSources);
        assertEquals(2, dataSources.size());
        assertTrue(dataSources.stream().anyMatch(ds -> ds.getName().equals("source1")));
        assertTrue(dataSources.stream().anyMatch(ds -> ds.getName().equals("source2")));
    }
    
    @Test
    void testUpdateDataSource() {
        DataSource dataSource = new DataSource();
        dataSource.setName("kafka-source");
        dataSource.setType(DataSource.SourceType.KAFKA);
        dataSource.setDescription("Kafka data source");
        
        dataSourceService.createDataSource(dataSource);
        
        dataSource.setDescription("Updated Kafka data source");
        
        DataSource updatedDataSource = dataSourceService.updateDataSource(dataSource);
        
        assertNotNull(updatedDataSource);
        assertEquals("kafka-source", updatedDataSource.getName());
        assertEquals(DataSource.SourceType.KAFKA, updatedDataSource.getType());
        assertEquals("Updated Kafka data source", updatedDataSource.getDescription());
        
        Optional<DataSource> retrievedDataSource = dataSourceService.getDataSource("kafka-source");
        assertTrue(retrievedDataSource.isPresent());
        assertEquals("Updated Kafka data source", retrievedDataSource.get().getDescription());
    }
    
    @Test
    void testDeleteDataSource() {
        DataSource dataSource = new DataSource();
        dataSource.setName("kafka-source");
        dataSource.setType(DataSource.SourceType.KAFKA);
        
        dataSourceService.createDataSource(dataSource);
        
        assertTrue(dataSourceService.getDataSource("kafka-source").isPresent());
        
        boolean deleted = dataSourceService.deleteDataSource("kafka-source");
        
        assertTrue(deleted);
        
        assertFalse(dataSourceService.getDataSource("kafka-source").isPresent());
        
        boolean notDeleted = dataSourceService.deleteDataSource("nonexistent");
        assertFalse(notDeleted);
    }
    
    @Test
    void testDataSourceWithProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("topic", "test-topic");
        properties.put("group.id", "test-group");
        
        DataSource dataSource = new DataSource();
        dataSource.setName("kafka-source");
        dataSource.setType(DataSource.SourceType.KAFKA);
        dataSource.setProperties(properties);
        dataSource.setDescription("Kafka data source with properties");
        
        DataSource createdDataSource = dataSourceService.createDataSource(dataSource);
        
        assertNotNull(createdDataSource);
        assertEquals("kafka-source", createdDataSource.getName());
        assertEquals(DataSource.SourceType.KAFKA, createdDataSource.getType());
        assertEquals(3, createdDataSource.getProperties().size());
        assertEquals("localhost:9092", createdDataSource.getProperties().get("bootstrap.servers"));
        assertEquals("test-topic", createdDataSource.getProperties().get("topic"));
        assertEquals("test-group", createdDataSource.getProperties().get("group.id"));
        assertEquals("Kafka data source with properties", createdDataSource.getDescription());
    }
}
