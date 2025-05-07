package com.gs.datalakehouse.core.service;

import com.gs.datalakehouse.core.model.Column;
import com.gs.datalakehouse.core.model.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TableServiceTest {

    @Mock
    private TableService tableService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCreateTable() {
        // Create columns
        Column idColumn = new Column("id", "string", false, "Primary key");
        Column nameColumn = new Column("name", "string", false, "Customer name");
        Column emailColumn = new Column("email", "string", true, "Customer email");
        
        List<Column> columns = Arrays.asList(idColumn, nameColumn, emailColumn);
        
        // Create table object
        Table table = new Table();
        table.setName("customers");
        table.setDatabase("default");
        table.setColumns(columns);
        table.setFormat("parquet");
        table.setDescription("Customer table");
        
        // Mock service behavior
        when(tableService.createTable(any(Table.class))).thenReturn(table);
        
        // Call service method
        Table createdTable = tableService.createTable(table);
        
        // Verify table properties
        assertNotNull(createdTable);
        assertEquals("default", createdTable.getDatabase());
        assertEquals("customers", createdTable.getName());
        assertEquals(3, createdTable.getColumns().size());
        assertEquals("parquet", createdTable.getFormat());
        assertEquals("Customer table", createdTable.getDescription());
        
        // Verify columns were added correctly
        assertTrue(createdTable.getColumns().contains(idColumn));
        assertTrue(createdTable.getColumns().contains(nameColumn));
        assertTrue(createdTable.getColumns().contains(emailColumn));
    }
    
    @Test
    void testGetTable() {
        // Create a table
        Table table = new Table();
        table.setName("products");
        table.setDatabase("default");
        table.setFormat("parquet");
        table.setDescription("Product table");
        
        // Mock service behavior
        when(tableService.getTable("default", "products")).thenReturn(Optional.of(table));
        
        // Call service method
        Optional<Table> retrievedTable = tableService.getTable("default", "products");
        
        // Verify retrieved table
        assertTrue(retrievedTable.isPresent());
        assertEquals("default", retrievedTable.get().getDatabase());
        assertEquals("products", retrievedTable.get().getName());
        assertEquals("parquet", retrievedTable.get().getFormat());
        assertEquals("Product table", retrievedTable.get().getDescription());
    }
    
    @Test
    void testListTables() {
        // Create tables
        Table table1 = new Table();
        table1.setName("table1");
        table1.setDatabase("default");
        
        Table table2 = new Table();
        table2.setName("table2");
        table2.setDatabase("default");
        
        List<Table> defaultTables = Arrays.asList(table1, table2);
        
        // Mock service behavior
        when(tableService.listTables("default")).thenReturn(defaultTables);
        
        // Call service method
        List<Table> retrievedTables = tableService.listTables("default");
        
        // Verify tables
        assertNotNull(retrievedTables);
        assertEquals(2, retrievedTables.size());
        assertEquals("table1", retrievedTables.get(0).getName());
        assertEquals("table2", retrievedTables.get(1).getName());
    }
    
    @Test
    void testUpdateTable() {
        // Create a table
        Table table = new Table();
        table.setName("customers");
        table.setDatabase("default");
        table.setFormat("parquet");
        table.setDescription("Customer table");
        
        // Update description
        table.setDescription("Updated customer table");
        
        // Mock service behavior
        when(tableService.updateTable(any(Table.class))).thenReturn(table);
        
        // Call service method
        Table updatedTable = tableService.updateTable(table);
        
        // Verify updated table
        assertNotNull(updatedTable);
        assertEquals("default", updatedTable.getDatabase());
        assertEquals("customers", updatedTable.getName());
        assertEquals("Updated customer table", updatedTable.getDescription());
    }
    
    @Test
    void testDeleteTable() {
        // Mock service behavior
        when(tableService.deleteTable("default", "customers")).thenReturn(true);
        when(tableService.deleteTable("default", "nonexistent")).thenReturn(false);
        
        // Call service method
        boolean deleted = tableService.deleteTable("default", "customers");
        boolean notDeleted = tableService.deleteTable("default", "nonexistent");
        
        // Verify deletion results
        assertTrue(deleted);
        assertFalse(notDeleted);
    }
}
