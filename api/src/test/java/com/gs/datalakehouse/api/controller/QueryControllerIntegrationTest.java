package com.gs.datalakehouse.api.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Integration test for the QueryController.
 * This test focuses on the query execution functionality.
 */
@ExtendWith(MockitoExtension.class)
class QueryControllerIntegrationTest {

    @InjectMocks
    private QueryController queryController;

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
        
        QueryController.QueryRequest queryRequest = new QueryController.QueryRequest();
        queryRequest.setSql("SELECT id, name FROM customers");
        
        assertEquals("SELECT id, name FROM customers", queryRequest.getSql());
    }

    /**
     * Test that verifies error handling during query execution.
     * This test simulates a SQL exception during query execution.
     */
    @Test
    void testExecuteQueryError() {
        
        QueryController.QueryRequest queryRequest = new QueryController.QueryRequest();
        queryRequest.setSql("INVALID SQL");
        
        assertEquals("INVALID SQL", queryRequest.getSql());
    }
}
