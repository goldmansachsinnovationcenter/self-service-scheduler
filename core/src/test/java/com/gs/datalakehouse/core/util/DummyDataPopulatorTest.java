package com.gs.datalakehouse.core.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for the DummyDataPopulator class.
 * Uses reflection and mocking to test the SQL operations without actual database connections.
 */
class DummyDataPopulatorTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockStatement;

    private DummyDataPopulator populator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        populator = spy(new DummyDataPopulator(""));
    }

    /**
     * Test subclass that overrides the database operations to avoid actual connections.
     */
    private static class TestDummyDataPopulator extends DummyDataPopulator {
        private boolean customersPopulated = false;
        private boolean productsPopulated = false;
        private boolean transactionsPopulated = false;
        
        public TestDummyDataPopulator() {
            super("");
        }
        
        @Override
        protected void populateCustomers() throws SQLException {
            customersPopulated = true;
        }
        
        @Override
        protected void populateProducts() throws SQLException {
            productsPopulated = true;
        }
        
        @Override
        protected void populateTransactions() throws SQLException {
            transactionsPopulated = true;
        }
        
        public boolean isCustomersPopulated() {
            return customersPopulated;
        }
        
        public boolean isProductsPopulated() {
            return productsPopulated;
        }
        
        public boolean isTransactionsPopulated() {
            return transactionsPopulated;
        }
    }

    @Test
    void testPopulateDummyData() {
        TestDummyDataPopulator testPopulator = new TestDummyDataPopulator();
        
        try {
            testPopulator.populateDummyData();
            
            assertTrue(testPopulator.isCustomersPopulated(), "Customers should be populated");
            assertTrue(testPopulator.isProductsPopulated(), "Products should be populated");
            assertTrue(testPopulator.isTransactionsPopulated(), "Transactions should be populated");
        } catch (Exception e) {
            fail("Exception occurred while calling populateDummyData: " + e.getMessage());
        }
    }
    
    @Test
    void testPopulateDummyDataWithException() throws SQLException {
        doThrow(new SQLException("Test exception")).when(populator).populateCustomers();
        
        populator.populateDummyData();
        
        verify(populator).populateCustomers();
        verify(populator, never()).populateProducts();
        verify(populator, never()).populateTransactions();
    }
    
    @Test
    void testConstructor() {
        DummyDataPopulator populator = new DummyDataPopulator("");
        
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
    
    @Test
    void testPopulateCustomers() throws Exception {
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        
        Method method = DummyDataPopulator.class.getDeclaredMethod("populateCustomers");
        method.setAccessible(true);
        method.invoke(populator);
        
        verify(mockConnection).prepareStatement(contains("INSERT INTO customers"));
        verify(mockStatement, times(5)).addBatch();
        verify(mockStatement).executeBatch();
        verify(mockStatement).close();
        verify(mockConnection).close();
    }
    
    @Test
    void testPopulateProducts() throws Exception {
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        
        Method method = DummyDataPopulator.class.getDeclaredMethod("populateProducts");
        method.setAccessible(true);
        method.invoke(populator);
        
        verify(mockConnection).prepareStatement(contains("INSERT INTO products"));
        verify(mockStatement, times(5)).addBatch();
        verify(mockStatement).executeBatch();
        verify(mockStatement).close();
        verify(mockConnection).close();
    }
    
    @Test
    void testPopulateTransactions() throws Exception {
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        
        Method method = DummyDataPopulator.class.getDeclaredMethod("populateTransactions");
        method.setAccessible(true);
        method.invoke(populator);
        
        verify(mockConnection).prepareStatement(contains("INSERT INTO transactions"));
        verify(mockStatement, times(8)).addBatch();
        verify(mockStatement).executeBatch();
        verify(mockStatement).close();
        verify(mockConnection).close();
    }
    
    @Test
    void testPopulateCustomersWithSQLException() throws Exception {
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("Test exception"));
        
        Method method = DummyDataPopulator.class.getDeclaredMethod("populateCustomers");
        method.setAccessible(true);
        
        assertThrows(InvocationTargetException.class, () -> method.invoke(populator));
    }
    
    @Test
    void testSqlStatements() {
        try {
            Method populateCustomersMethod = DummyDataPopulator.class.getDeclaredMethod("populateCustomers");
            Method populateProductsMethod = DummyDataPopulator.class.getDeclaredMethod("populateProducts");
            Method populateTransactionsMethod = DummyDataPopulator.class.getDeclaredMethod("populateTransactions");
            Method getConnectionMethod = DummyDataPopulator.class.getDeclaredMethod("getConnection");
            
            assertNotNull(populateCustomersMethod, "populateCustomers method should exist");
            assertNotNull(populateProductsMethod, "populateProducts method should exist");
            assertNotNull(populateTransactionsMethod, "populateTransactions method should exist");
            assertNotNull(getConnectionMethod, "getConnection method should exist");
        } catch (NoSuchMethodException e) {
            fail("Required methods do not exist: " + e.getMessage());
        }
    }
    
    @Test
    void testSqlExecutionWithMockConnection() throws SQLException {
        class SqlCapturingDataPopulator extends DummyDataPopulator {
            boolean customersMethodCalled = false;
            boolean productsMethodCalled = false;
            boolean transactionsMethodCalled = false;
            
            public SqlCapturingDataPopulator() {
                super("");
            }
            
            @Override
            protected void populateCustomers() throws SQLException {
                customersMethodCalled = true;
            }
            
            @Override
            protected void populateProducts() throws SQLException {
                productsMethodCalled = true;
            }
            
            @Override
            protected void populateTransactions() throws SQLException {
                transactionsMethodCalled = true;
            }
            
            @Override
            protected Connection getConnection() throws SQLException {
                return null;
            }
        }
        
        SqlCapturingDataPopulator testPopulator = new SqlCapturingDataPopulator();
        testPopulator.populateDummyData();
        
        assertTrue(testPopulator.customersMethodCalled, "populateCustomers method should be called");
        assertTrue(testPopulator.productsMethodCalled, "populateProducts method should be called");
        assertTrue(testPopulator.transactionsMethodCalled, "populateTransactions method should be called");
    }
    
    @Test
    void testGetConnection() throws SQLException {
        DummyDataPopulator badPopulator = new DummyDataPopulator("") {
            @Override
            protected Connection getConnection() throws SQLException {
                throw new SQLException("Connection failed");
            }
        };
        
        assertThrows(SQLException.class, () -> badPopulator.getConnection());
        
        try {
            Method getConnectionMethod = DummyDataPopulator.class.getDeclaredMethod("getConnection");
            getConnectionMethod.setAccessible(true);
            
            try {
                getConnectionMethod.invoke(populator);
            } catch (InvocationTargetException e) {
                if (!(e.getCause() instanceof SQLException)) {
                    fail("Expected SQLException but got: " + e.getCause());
                }
            }
        } catch (Exception e) {
            if (!(e instanceof SQLException || e.getCause() instanceof SQLException)) {
                fail("Unexpected exception: " + e);
            }
        }
    }
    
    @Test
    void testStatementSetters() throws Exception {
        doReturn(mockConnection).when(populator).getConnection();
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
        
        Method method = DummyDataPopulator.class.getDeclaredMethod("populateCustomers");
        method.setAccessible(true);
        method.invoke(populator);
        
        verify(mockStatement, times(5)).setString(eq(1), anyString()); // customer_id
        verify(mockStatement, times(5)).setString(eq(2), anyString()); // name
        verify(mockStatement, times(5)).setString(eq(3), anyString()); // email
        verify(mockStatement, times(5)).setString(eq(4), anyString()); // phone
        verify(mockStatement, times(5)).setString(eq(5), anyString()); // address
        verify(mockStatement, times(5)).setString(eq(6), anyString()); // registration_date
    }
}
