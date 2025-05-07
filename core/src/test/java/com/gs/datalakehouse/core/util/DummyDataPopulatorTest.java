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
            
            assertNotNull(populateCustomersMethod, "populateCustomers method should exist");
            assertNotNull(populateProductsMethod, "populateProducts method should exist");
            assertNotNull(populateTransactionsMethod, "populateTransactions method should exist");
        } catch (NoSuchMethodException e) {
            fail("Required methods do not exist: " + e.getMessage());
        }
    }
}
