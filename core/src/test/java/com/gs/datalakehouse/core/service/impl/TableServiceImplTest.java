package com.gs.datalakehouse.core.service.impl;

import com.gs.datalakehouse.core.model.Column;
import com.gs.datalakehouse.core.model.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TableServiceImplTest {

    private TableServiceImpl tableService;

    @BeforeEach
    void setUp() {
        tableService = new TableServiceImpl();
    }

    @Test
    void testCreateTable() {
        Column idColumn = new Column("id", "string", false, "Primary key");
        Column nameColumn = new Column("name", "string", false, "Customer name");
        Column emailColumn = new Column("email", "string", true, "Customer email");
        
        List<Column> columns = Arrays.asList(idColumn, nameColumn, emailColumn);
        
        Table table = new Table();
        table.setName("customers");
        table.setDatabase("default");
        table.setColumns(columns);
        table.setFormat("parquet");
        table.setDescription("Customer table");
        
        Table createdTable = tableService.createTable(table);
        
        assertNotNull(createdTable);
        assertEquals("default", createdTable.getDatabase());
        assertEquals("customers", createdTable.getName());
        assertEquals(3, createdTable.getColumns().size());
        assertEquals("parquet", createdTable.getFormat());
        assertEquals("Customer table", createdTable.getDescription());
        
        Optional<Table> retrievedTable = tableService.getTable("default", "customers");
        assertTrue(retrievedTable.isPresent());
        assertEquals("customers", retrievedTable.get().getName());
    }
    
    @Test
    void testGetTable() {
        Table table = new Table();
        table.setName("products");
        table.setDatabase("default");
        table.setFormat("parquet");
        table.setDescription("Product table");
        
        tableService.createTable(table);
        
        Optional<Table> retrievedTable = tableService.getTable("default", "products");
        
        assertTrue(retrievedTable.isPresent());
        assertEquals("default", retrievedTable.get().getDatabase());
        assertEquals("products", retrievedTable.get().getName());
        assertEquals("parquet", retrievedTable.get().getFormat());
        assertEquals("Product table", retrievedTable.get().getDescription());
        
        Optional<Table> nonExistentTable = tableService.getTable("default", "nonexistent");
        assertFalse(nonExistentTable.isPresent());
    }
    
    @Test
    void testListTables() {
        Table table1 = new Table();
        table1.setName("table1");
        table1.setDatabase("default");
        
        Table table2 = new Table();
        table2.setName("table2");
        table2.setDatabase("default");
        
        Table table3 = new Table();
        table3.setName("table3");
        table3.setDatabase("other");
        
        tableService.createTable(table1);
        tableService.createTable(table2);
        tableService.createTable(table3);
        
        List<Table> defaultTables = tableService.listTables("default");
        List<Table> otherTables = tableService.listTables("other");
        
        assertNotNull(defaultTables);
        assertEquals(2, defaultTables.size());
        assertTrue(defaultTables.stream().anyMatch(t -> t.getName().equals("table1")));
        assertTrue(defaultTables.stream().anyMatch(t -> t.getName().equals("table2")));
        
        assertNotNull(otherTables);
        assertEquals(1, otherTables.size());
        assertEquals("table3", otherTables.get(0).getName());
    }
    
    @Test
    void testUpdateTable() {
        Table table = new Table();
        table.setName("customers");
        table.setDatabase("default");
        table.setFormat("parquet");
        table.setDescription("Customer table");
        
        tableService.createTable(table);
        
        table.setDescription("Updated customer table");
        
        Table updatedTable = tableService.updateTable(table);
        
        assertNotNull(updatedTable);
        assertEquals("default", updatedTable.getDatabase());
        assertEquals("customers", updatedTable.getName());
        assertEquals("Updated customer table", updatedTable.getDescription());
        
        Optional<Table> retrievedTable = tableService.getTable("default", "customers");
        assertTrue(retrievedTable.isPresent());
        assertEquals("Updated customer table", retrievedTable.get().getDescription());
    }
    
    @Test
    void testDeleteTable() {
        Table table = new Table();
        table.setName("customers");
        table.setDatabase("default");
        
        tableService.createTable(table);
        
        assertTrue(tableService.getTable("default", "customers").isPresent());
        
        boolean deleted = tableService.deleteTable("default", "customers");
        
        assertTrue(deleted);
        
        assertFalse(tableService.getTable("default", "customers").isPresent());
        
        boolean notDeleted = tableService.deleteTable("default", "nonexistent");
        assertFalse(notDeleted);
    }
    
    @Test
    void testTableWithFullConstructor() {
        Column idColumn = new Column("id", "string", false, "Primary key");
        Column nameColumn = new Column("name", "string", false, "Customer name");
        List<Column> columns = Arrays.asList(idColumn, nameColumn);
        
        List<String> partitionBy = Arrays.asList("id");
        
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        
        Table table = new Table(
            "customers",
            "default",
            "/warehouse/default/customers",
            columns,
            partitionBy,
            properties,
            "parquet",
            "Customer table"
        );
        
        Table createdTable = tableService.createTable(table);
        
        assertNotNull(createdTable);
        assertEquals("default", createdTable.getDatabase());
        assertEquals("customers", createdTable.getName());
        assertEquals("/warehouse/default/customers", createdTable.getLocation());
        assertEquals(2, createdTable.getColumns().size());
        assertEquals(1, createdTable.getPartitionBy().size());
        assertEquals("id", createdTable.getPartitionBy().get(0));
        assertEquals(1, createdTable.getProperties().size());
        assertEquals("2", createdTable.getProperties().get("format-version"));
        assertEquals("parquet", createdTable.getFormat());
        assertEquals("Customer table", createdTable.getDescription());
    }
}
