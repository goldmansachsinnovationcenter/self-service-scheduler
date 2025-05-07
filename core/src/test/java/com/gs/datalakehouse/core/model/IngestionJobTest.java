package com.gs.datalakehouse.core.model;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class IngestionJobTest {

    @Test
    void testConstructorAndGetters() {
        Map<String, String> properties = new HashMap<>();
        properties.put("checkpoint.location", "hdfs://localhost:9000/checkpoints/job1");
        properties.put("batch.size", "1000");
        
        IngestionJob job = new IngestionJob(
            "job1", 
            "kafka-source", 
            "default.customers", 
            "com.gs.datalakehouse.transform.CustomerTransform",
            properties,
            "0 0 * * *", // Daily at midnight
            "Customer data ingestion job"
        );
        
        assertEquals("job1", job.getName());
        assertEquals("kafka-source", job.getSourceRef());
        assertEquals("default.customers", job.getTargetTableRef());
        assertEquals("com.gs.datalakehouse.transform.CustomerTransform", job.getTransformationClass());
        assertEquals(properties, job.getProperties());
        assertEquals("0 0 * * *", job.getSchedule());
        assertEquals("Customer data ingestion job", job.getDescription());
    }
    
    @Test
    void testSetters() {
        IngestionJob job = new IngestionJob();
        
        job.setName("job2");
        job.setSourceRef("file-source");
        job.setTargetTableRef("default.products");
        job.setTransformationClass("com.gs.datalakehouse.transform.ProductTransform");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "/data/products");
        properties.put("format", "parquet");
        job.setProperties(properties);
        
        job.setSchedule("0 */6 * * *"); // Every 6 hours
        job.setDescription("Product data ingestion job");
        
        assertEquals("job2", job.getName());
        assertEquals("file-source", job.getSourceRef());
        assertEquals("default.products", job.getTargetTableRef());
        assertEquals("com.gs.datalakehouse.transform.ProductTransform", job.getTransformationClass());
        assertEquals(properties, job.getProperties());
        assertEquals("0 */6 * * *", job.getSchedule());
        assertEquals("Product data ingestion job", job.getDescription());
    }
    
    @Test
    void testEquals() {
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("checkpoint.location", "hdfs://localhost:9000/checkpoints/job1");
        
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("path", "/data/products");
        
        IngestionJob job1 = new IngestionJob(
            "job1", 
            "kafka-source", 
            "default.customers", 
            "com.gs.datalakehouse.transform.CustomerTransform",
            properties1,
            "0 0 * * *",
            "Customer data ingestion job"
        );
        
        IngestionJob job2 = new IngestionJob(
            "job1", 
            "file-source", 
            "default.products", 
            "com.gs.datalakehouse.transform.ProductTransform",
            properties2,
            "0 */6 * * *",
            "Product data ingestion job"
        );
        
        IngestionJob job3 = new IngestionJob(
            "job3", 
            "kafka-source", 
            "default.customers", 
            "com.gs.datalakehouse.transform.CustomerTransform",
            properties1,
            "0 0 * * *",
            "Customer data ingestion job"
        );
        
        assertEquals(job1, job2);
        assertNotEquals(job1, job3);
        assertNotEquals(job2, job3);
        
        assertNotEquals(job1, null);
        assertNotEquals(job1, "not an ingestion job");
    }
    
    @Test
    void testHashCode() {
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("checkpoint.location", "hdfs://localhost:9000/checkpoints/job1");
        
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("path", "/data/products");
        
        IngestionJob job1 = new IngestionJob(
            "job1", 
            "kafka-source", 
            "default.customers", 
            "com.gs.datalakehouse.transform.CustomerTransform",
            properties1,
            "0 0 * * *",
            "Customer data ingestion job"
        );
        
        IngestionJob job2 = new IngestionJob(
            "job1", 
            "file-source", 
            "default.products", 
            "com.gs.datalakehouse.transform.ProductTransform",
            properties2,
            "0 */6 * * *",
            "Product data ingestion job"
        );
        
        assertEquals(job1.hashCode(), job2.hashCode());
    }
    
    @Test
    void testToString() {
        Map<String, String> properties = new HashMap<>();
        properties.put("checkpoint.location", "hdfs://localhost:9000/checkpoints/job1");
        
        IngestionJob job = new IngestionJob(
            "job1", 
            "kafka-source", 
            "default.customers", 
            "com.gs.datalakehouse.transform.CustomerTransform",
            properties,
            "0 0 * * *",
            "Customer data ingestion job"
        );
        
        String toString = job.toString();
        
        assertTrue(toString.contains("job1"));
        assertTrue(toString.contains("kafka-source"));
        assertTrue(toString.contains("default.customers"));
        assertTrue(toString.contains("com.gs.datalakehouse.transform.CustomerTransform"));
        assertTrue(toString.contains("checkpoint.location"));
        assertTrue(toString.contains("0 0 * * *"));
        assertTrue(toString.contains("Customer data ingestion job"));
    }
}
