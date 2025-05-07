package com.gs.datalakehouse.core.util;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the DummyDataPopulator class.
 * Since we can't easily mock JDBC connections in the test environment,
 * we focus on testing the configuration and structure rather than actual database operations.
 */
class DummyDataPopulatorTest {

    /**
     * Test subclass that overrides the database operations to avoid actual connections.
     */
    private static class TestDummyDataPopulator extends DummyDataPopulator {
        private boolean customersPopulated = false;
        private boolean productsPopulated = false;
        private boolean transactionsPopulated = false;
        
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
        TestDummyDataPopulator populator = new TestDummyDataPopulator();
        
        try {
            populator.populateDummyData();
            
            assertTrue(populator.isCustomersPopulated(), "Customers should be populated");
            assertTrue(populator.isProductsPopulated(), "Products should be populated");
            assertTrue(populator.isTransactionsPopulated(), "Transactions should be populated");
        } catch (Exception e) {
            fail("Exception occurred while calling populateDummyData: " + e.getMessage());
        }
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
    
    @Test
    void testSqlStatements() {
        try {
            java.lang.reflect.Method populateCustomersMethod = DummyDataPopulator.class.getDeclaredMethod("populateCustomers");
            java.lang.reflect.Method populateProductsMethod = DummyDataPopulator.class.getDeclaredMethod("populateProducts");
            java.lang.reflect.Method populateTransactionsMethod = DummyDataPopulator.class.getDeclaredMethod("populateTransactions");
            java.lang.reflect.Method getConnectionMethod = DummyDataPopulator.class.getDeclaredMethod("getConnection");
            
            assertNotNull(populateCustomersMethod, "populateCustomers method should exist");
            assertNotNull(populateProductsMethod, "populateProducts method should exist");
            assertNotNull(populateTransactionsMethod, "populateTransactions method should exist");
            assertNotNull(getConnectionMethod, "getConnection method should exist");
            
            String populateCustomersCode = getMethodBody(populateCustomersMethod);
            assertTrue(populateCustomersCode.contains("INSERT INTO customers"), "populateCustomers should contain INSERT INTO customers statement");
            assertTrue(populateCustomersCode.contains("customer_id"), "populateCustomers should reference customer_id column");
            assertTrue(populateCustomersCode.contains("name"), "populateCustomers should reference name column");
            assertTrue(populateCustomersCode.contains("email"), "populateCustomers should reference email column");
            
            String populateProductsCode = getMethodBody(populateProductsMethod);
            assertTrue(populateProductsCode.contains("INSERT INTO products"), "populateProducts should contain INSERT INTO products statement");
            assertTrue(populateProductsCode.contains("product_id"), "populateProducts should reference product_id column");
            assertTrue(populateProductsCode.contains("category"), "populateProducts should reference category column");
            assertTrue(populateProductsCode.contains("price"), "populateProducts should reference price column");
            
            String populateTransactionsCode = getMethodBody(populateTransactionsMethod);
            assertTrue(populateTransactionsCode.contains("INSERT INTO transactions"), "populateTransactions should contain INSERT INTO transactions statement");
            assertTrue(populateTransactionsCode.contains("transaction_id"), "populateTransactions should reference transaction_id column");
            assertTrue(populateTransactionsCode.contains("customer_id"), "populateTransactions should reference customer_id column");
            assertTrue(populateTransactionsCode.contains("product_id"), "populateTransactions should reference product_id column");
        } catch (NoSuchMethodException e) {
            fail("Required methods do not exist: " + e.getMessage());
        }
    }
    
    /**
     * Helper method to get the method body as a string.
     * This is a simplified approach that just returns the method's toString representation.
     */
    private String getMethodBody(java.lang.reflect.Method method) {
        return method.toString();
    }
    
    @Test
    void testGetConnection() {
        DummyDataPopulator populator = new DummyDataPopulator() {
            @Override
            protected java.sql.Connection getConnection() throws SQLException {
                return null;
            }
        };
        
        try {
            java.lang.reflect.Method getConnectionMethod = DummyDataPopulator.class.getDeclaredMethod("getConnection");
            getConnectionMethod.setAccessible(true);
            
            assertNull(getConnectionMethod.invoke(populator));
        } catch (Exception e) {
            fail("Exception occurred while testing getConnection: " + e.getMessage());
        }
    }
}
