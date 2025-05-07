package com.gs.datalakehouse.api.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class QueryControllerTest {

    @InjectMocks
    private QueryController queryController;

    @Test
    void testQueryRequestGetterSetter() {
        // Test the inner QueryRequest class
        String sql = "SELECT * FROM default.customers";
        
        QueryController.QueryRequest queryRequest = new QueryController.QueryRequest();
        queryRequest.setSql(sql);
        
        assertEquals(sql, queryRequest.getSql());
    }
}
