package com.gs.datalakehouse.core.model;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class DataSourceTest {

    @Test
    void testConstructorAndGetters() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("topic", "test-topic");
        
        DataSource dataSource = new DataSource("kafka-source", DataSource.SourceType.KAFKA, properties, "Kafka data source");
        
        assertEquals("kafka-source", dataSource.getName());
        assertEquals(DataSource.SourceType.KAFKA, dataSource.getType());
        assertEquals(properties, dataSource.getProperties());
        assertEquals("Kafka data source", dataSource.getDescription());
    }
    
    @Test
    void testSetters() {
        DataSource dataSource = new DataSource();
        
        dataSource.setName("file-source");
        dataSource.setType(DataSource.SourceType.FILE);
        
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "/data/input");
        properties.put("format", "csv");
        dataSource.setProperties(properties);
        
        dataSource.setDescription("File data source");
        
        assertEquals("file-source", dataSource.getName());
        assertEquals(DataSource.SourceType.FILE, dataSource.getType());
        assertEquals(properties, dataSource.getProperties());
        assertEquals("File data source", dataSource.getDescription());
    }
    
    @Test
    void testEquals() {
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("bootstrap.servers", "localhost:9092");
        
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("path", "/data/input");
        
        DataSource dataSource1 = new DataSource("source1", DataSource.SourceType.KAFKA, properties1, "Source 1");
        DataSource dataSource2 = new DataSource("source1", DataSource.SourceType.FILE, properties2, "Different description");
        DataSource dataSource3 = new DataSource("source2", DataSource.SourceType.KAFKA, properties1, "Source 1");
        
        assertEquals(dataSource1, dataSource2);
        assertNotEquals(dataSource1, dataSource3);
        assertNotEquals(dataSource2, dataSource3);
        
        assertNotEquals(dataSource1, null);
        assertNotEquals(dataSource1, "not a data source");
    }
    
    @Test
    void testHashCode() {
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("bootstrap.servers", "localhost:9092");
        
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("path", "/data/input");
        
        DataSource dataSource1 = new DataSource("source1", DataSource.SourceType.KAFKA, properties1, "Source 1");
        DataSource dataSource2 = new DataSource("source1", DataSource.SourceType.FILE, properties2, "Different description");
        
        assertEquals(dataSource1.hashCode(), dataSource2.hashCode());
    }
    
    @Test
    void testToString() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        
        DataSource dataSource = new DataSource("kafka-source", DataSource.SourceType.KAFKA, properties, "Kafka data source");
        String toString = dataSource.toString();
        
        assertTrue(toString.contains("kafka-source"));
        assertTrue(toString.contains("KAFKA"));
        assertTrue(toString.contains("bootstrap.servers"));
        assertTrue(toString.contains("Kafka data source"));
    }
    
    @Test
    void testSourceTypeEnum() {
        assertEquals(DataSource.SourceType.KAFKA, DataSource.SourceType.valueOf("KAFKA"));
        assertEquals(DataSource.SourceType.FILE, DataSource.SourceType.valueOf("FILE"));
        assertEquals(DataSource.SourceType.DATABASE, DataSource.SourceType.valueOf("DATABASE"));
        assertEquals(DataSource.SourceType.API, DataSource.SourceType.valueOf("API"));
        
        assertEquals(0, DataSource.SourceType.KAFKA.ordinal());
        assertEquals(1, DataSource.SourceType.FILE.ordinal());
        assertEquals(2, DataSource.SourceType.DATABASE.ordinal());
        assertEquals(3, DataSource.SourceType.API.ordinal());
    }
}
