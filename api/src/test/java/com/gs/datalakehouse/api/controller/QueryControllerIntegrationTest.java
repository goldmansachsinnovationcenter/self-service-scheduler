package com.gs.datalakehouse.api.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.*;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Integration test for the QueryController.
 * This test focuses on the query execution functionality.
 */
@ExtendWith(MockitoExtension.class)
class QueryControllerIntegrationTest {

    @InjectMocks
    private QueryController queryController;

    @Mock
    private Connection mockConnection;
    
    @Mock
    private Statement mockStatement;
    
    @Mock
    private ResultSet mockResultSet;
    
    @Mock
    private ResultSetMetaData mockMetaData;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(queryController, "trinoHost", "localhost");
        ReflectionTestUtils.setField(queryController, "trinoPort", 8080);
        ReflectionTestUtils.setField(queryController, "trinoUser", "test");
        ReflectionTestUtils.setField(queryController, "trinoCatalog", "iceberg");
        ReflectionTestUtils.setField(queryController, "trinoSchema", "default");
    }

    /**
     * Test that verifies the query execution functionality with mocked JDBC components.
     * This test mocks the JDBC connection, statement, and result set to simulate a successful query execution.
     */
    @Test
    void testExecuteQuerySuccess() throws Exception {
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class)) {
            driverManagerMock.when(() -> 
                DriverManager.getConnection(anyString(), anyString(), eq(null)))
                .thenReturn(mockConnection);
            
            when(mockConnection.createStatement()).thenReturn(mockStatement);
            
            when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
            
            when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
            
            when(mockMetaData.getColumnCount()).thenReturn(2);
            when(mockMetaData.getColumnName(1)).thenReturn("id");
            when(mockMetaData.getColumnName(2)).thenReturn("name");
            
            when(mockResultSet.next()).thenReturn(true, true, false); // Two rows of data
            when(mockResultSet.getObject(1)).thenReturn("1", "2");
            when(mockResultSet.getObject(2)).thenReturn("John", "Jane");
            
            QueryController.QueryRequest queryRequest = new QueryController.QueryRequest();
            queryRequest.setSql("SELECT id, name FROM customers");
            
            ResponseEntity<Map<String, Object>> response = queryController.executeQuery(queryRequest);
            
            assertEquals(HttpStatus.OK, response.getStatusCode());
            assertNotNull(response.getBody());
            
            List<String> columns = (List<String>) response.getBody().get("columns");
            assertEquals(2, columns.size());
            assertEquals("id", columns.get(0));
            assertEquals("name", columns.get(1));
            
            List<Map<String, Object>> rows = (List<Map<String, Object>>) response.getBody().get("rows");
            assertEquals(2, rows.size());
            
            Map<String, Object> row1 = rows.get(0);
            assertEquals("1", row1.get("id"));
            assertEquals("John", row1.get("name"));
            
            Map<String, Object> row2 = rows.get(1);
            assertEquals("2", row2.get("id"));
            assertEquals("Jane", row2.get("name"));
        }
    }

    /**
     * Test that verifies error handling during query execution.
     * This test simulates a SQL exception during query execution.
     */
    @Test
    void testExecuteQueryError() throws Exception {
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class)) {
            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), anyString(), eq(null)))
                             .thenThrow(new SQLException("Connection error"));
            
            QueryController.QueryRequest queryRequest = new QueryController.QueryRequest();
            queryRequest.setSql("INVALID SQL");
            
            ResponseEntity<Map<String, Object>> response = queryController.executeQuery(queryRequest);
            
            assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
            assertNotNull(response.getBody());
            assertEquals("Connection error", response.getBody().get("error"));
        }
    }
}
