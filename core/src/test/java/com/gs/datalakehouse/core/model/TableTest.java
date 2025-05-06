package com.gs.datalakehouse.core.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TableTest {

    @Test
    public void testTableCreation() {
        String name = "test_table";
        String database = "test_db";
        String location = "hdfs://localhost:9000/warehouse/test_db/test_table";
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", "string", false, "ID column"));
        columns.add(new Column("value", "int", true, "Value column"));
        
        List<String> partitionBy = new ArrayList<>();
        partitionBy.add("id");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        
        String format = "parquet";
        String description = "Test table";

        Table table = new Table(name, database, location, columns, partitionBy, properties, format, description);

        assertEquals(name, table.getName());
        assertEquals(database, table.getDatabase());
        assertEquals(location, table.getLocation());
        assertEquals(2, table.getColumns().size());
        assertEquals(1, table.getPartitionBy().size());
        assertEquals("id", table.getPartitionBy().get(0));
        assertEquals("2", table.getProperties().get("format-version"));
        assertEquals(format, table.getFormat());
        assertEquals(description, table.getDescription());
    }

    @Test
    public void testTableEquality() {
        Table table1 = new Table("table1", "db1", null, null, null, null, null, null);
        Table table2 = new Table("table1", "db1", null, null, null, null, null, null);
        Table table3 = new Table("table2", "db1", null, null, null, null, null, null);
        Table table4 = new Table("table1", "db2", null, null, null, null, null, null);

        assertEquals(table1, table2);
        assertNotEquals(table1, table3);
        assertNotEquals(table1, table4);
        assertNotEquals(table3, table4);
    }

    @Test
    public void testTableHashCode() {
        Table table1 = new Table("table1", "db1", null, null, null, null, null, null);
        Table table2 = new Table("table1", "db1", null, null, null, null, null, null);

        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testTableToString() {
        Table table = new Table("table1", "db1", "location", null, null, null, "parquet", "description");

        String toString = table.toString();

        assertTrue(toString.contains("table1"));
        assertTrue(toString.contains("db1"));
        assertTrue(toString.contains("location"));
        assertTrue(toString.contains("parquet"));
        assertTrue(toString.contains("description"));
    }
}
