package com.gs.datalakehouse.api.controller;

import com.gs.datalakehouse.core.model.Column;
import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.TableService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TableControllerTest {

    @Mock
    private TableService tableService;

    @InjectMocks
    private TableController tableController;

    @Test
    void testGetTable() {
        // Create mock table
        Table mockTable = new Table();
        mockTable.setName("customers");
        mockTable.setDatabase("default");
        mockTable.setFormat("parquet");
        mockTable.setDescription("Customer table");
        
        // Mock service behavior
        when(tableService.getTable("default", "customers")).thenReturn(Optional.of(mockTable));
        
        // Call controller method
        ResponseEntity<Table> response = tableController.getTable("default", "customers");
        
        // Verify response
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockTable, response.getBody());
        
        // Verify service was called
        verify(tableService).getTable("default", "customers");
    }
    
    @Test
    void testGetTableNotFound() {
        // Mock service behavior for non-existent table
        when(tableService.getTable("default", "nonexistent")).thenReturn(Optional.empty());
        
        // Call controller method
        ResponseEntity<Table> response = tableController.getTable("default", "nonexistent");
        
        // Verify response
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
        
        // Verify service was called
        verify(tableService).getTable("default", "nonexistent");
    }
    
    @Test
    void testListTables() {
        // Create mock tables
        Table table1 = new Table();
        table1.setName("table1");
        table1.setDatabase("default");
        
        Table table2 = new Table();
        table2.setName("table2");
        table2.setDatabase("default");
        
        List<Table> mockTables = Arrays.asList(table1, table2);
        
        // Mock service behavior
        when(tableService.listTables("default")).thenReturn(mockTables);
        
        // Call controller method
        ResponseEntity<List<Table>> response = tableController.listTables("default");
        
        // Verify response
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockTables, response.getBody());
        assertEquals(2, response.getBody().size());
        
        // Verify service was called
        verify(tableService).listTables("default");
    }
    
    @Test
    void testCreateTable() {
        // Create mock table
        Column idColumn = new Column("id", "string", false, "Primary key");
        List<Column> columns = Arrays.asList(idColumn);
        
        Table mockTable = new Table();
        mockTable.setName("customers");
        mockTable.setDatabase("default");
        mockTable.setColumns(columns);
        mockTable.setFormat("parquet");
        mockTable.setDescription("Customer table");
        
        // Mock service behavior
        when(tableService.createTable(any(Table.class))).thenReturn(mockTable);
        
        // Call controller method
        ResponseEntity<Table> response = tableController.createTable(mockTable);
        
        // Verify response
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(mockTable, response.getBody());
        
        // Verify service was called
        verify(tableService).createTable(any(Table.class));
    }
    
    @Test
    void testUpdateTable() {
        // Create mock table
        Table mockTable = new Table();
        mockTable.setName("customers");
        mockTable.setDatabase("default");
        mockTable.setFormat("parquet");
        mockTable.setDescription("Updated customer table");
        
        // Mock service behavior
        when(tableService.updateTable(any(Table.class))).thenReturn(mockTable);
        
        // Call controller method
        ResponseEntity<Table> response = tableController.updateTable("default", "customers", mockTable);
        
        // Verify response
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockTable, response.getBody());
        
        // Verify service was called
        verify(tableService).updateTable(any(Table.class));
    }
    
    @Test
    void testUpdateTableBadRequest() {
        // Create mock table with mismatched name
        Table mockTable = new Table();
        mockTable.setName("different");
        mockTable.setDatabase("default");
        
        // Call controller method
        ResponseEntity<Table> response = tableController.updateTable("default", "customers", mockTable);
        
        // Verify response
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        
        // Verify service was not called
        verify(tableService, never()).updateTable(any(Table.class));
    }
    
    @Test
    void testDeleteTable() {
        // Mock service behavior
        when(tableService.deleteTable("default", "customers")).thenReturn(true);
        
        // Call controller method
        ResponseEntity<Void> response = tableController.deleteTable("default", "customers");
        
        // Verify response
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        
        // Verify service was called
        verify(tableService).deleteTable("default", "customers");
    }
    
    @Test
    void testDeleteTableNotFound() {
        // Mock service behavior
        when(tableService.deleteTable("default", "nonexistent")).thenReturn(false);
        
        // Call controller method
        ResponseEntity<Void> response = tableController.deleteTable("default", "nonexistent");
        
        // Verify response
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        
        // Verify service was called
        verify(tableService).deleteTable("default", "nonexistent");
    }
}
