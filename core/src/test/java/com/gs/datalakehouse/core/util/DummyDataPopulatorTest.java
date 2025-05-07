package com.gs.datalakehouse.core.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DummyDataPopulatorTest {

    @InjectMocks
    private DummyDataPopulator dummyDataPopulator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testPopulateDummyData() {
        DummyDataPopulator populator = spy(new DummyDataPopulator());
        
        doNothing().when(populator).populateCustomers();
        doNothing().when(populator).populateProducts();
        doNothing().when(populator).populateTransactions();
        
        populator.populateDummyData();
        
        verify(populator, times(1)).populateCustomers();
        verify(populator, times(1)).populateProducts();
        verify(populator, times(1)).populateTransactions();
    }
    
    @Test
    void testPopulateCustomers() throws Exception {
        DummyDataPopulator populator = spy(new DummyDataPopulator());
        
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        
        java.lang.reflect.Method method = DummyDataPopulator.class.getDeclaredMethod("populateCustomers");
        method.setAccessible(true);
        method.invoke(populator);
        
        verify(mockConnection, times(1)).prepareStatement(anyString());
        verify(mockStatement, times(5)).addBatch();
        verify(mockStatement, times(1)).executeBatch();
        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }
    
    @Test
    void testPopulateProducts() throws Exception {
        DummyDataPopulator populator = spy(new DummyDataPopulator());
        
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        
        java.lang.reflect.Method method = DummyDataPopulator.class.getDeclaredMethod("populateProducts");
        method.setAccessible(true);
        method.invoke(populator);
        
        verify(mockConnection, times(1)).prepareStatement(anyString());
        verify(mockStatement, times(5)).addBatch();
        verify(mockStatement, times(1)).executeBatch();
        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }
    
    @Test
    void testPopulateTransactions() throws Exception {
        DummyDataPopulator populator = spy(new DummyDataPopulator());
        
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockStatement = mock(PreparedStatement.class);
        
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        
        java.lang.reflect.Method method = DummyDataPopulator.class.getDeclaredMethod("populateTransactions");
        method.setAccessible(true);
        method.invoke(populator);
        
        verify(mockConnection, times(1)).prepareStatement(anyString());
        verify(mockStatement, times(8)).addBatch();
        verify(mockStatement, times(1)).executeBatch();
        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }
    
    @Test
    void testGetConnection() throws Exception {
        
        java.lang.reflect.Method method = DummyDataPopulator.class.getDeclaredMethod("getConnection");
        method.setAccessible(true);
        
        assertNotNull(method);
    }
    
    @Test
    void testConstructor() {
        DummyDataPopulator populator = new DummyDataPopulator();
        
        try {
            java.lang.reflect.Field jdbcUrlField = DummyDataPopulator.class.getDeclaredField("jdbcUrl");
            jdbcUrlField.setAccessible(true);
            String jdbcUrl = (String) jdbcUrlField.get(populator);
            
            java.lang.reflect.Field usernameField = DummyDataPopulator.class.getDeclaredField("username");
            usernameField.setAccessible(true);
            String username = (String) usernameField.get(populator);
            
            java.lang.reflect.Field passwordField = DummyDataPopulator.class.getDeclaredField("password");
            passwordField.setAccessible(true);
            String password = (String) passwordField.get(populator);
            
            assertEquals("jdbc:trino://localhost:8080/iceberg/default", jdbcUrl);
            assertEquals("trino", username);
            assertEquals("", password);
        } catch (Exception e) {
            fail("Exception occurred while accessing fields: " + e.getMessage());
        }
    }
}
