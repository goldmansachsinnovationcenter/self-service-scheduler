package com.gs.datalakehouse.core.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Utility class to populate the tables created by DummyDataGenerator with sample data.
 * This class inserts sample data into the customers, products, and transactions tables.
 */
@Component
public class DummyDataPopulator {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    @Autowired
    public DummyDataPopulator() {
        this.jdbcUrl = "jdbc:trino://localhost:8080/iceberg/default";
        this.username = "trino";
        this.password = "";
    }

    /**
     * Initializes the tables with sample data.
     */
    @PostConstruct
    public void populateDummyData() {
        try {
            populateCustomers();
            populateProducts();
            populateTransactions();
            System.out.println("Sample data populated successfully.");
        } catch (SQLException e) {
            System.err.println("Error populating sample data: " + e.getMessage());
        }
    }

    /**
     * Populates the customers table with sample data.
     */
    protected void populateCustomers() throws SQLException {
        String sql = "INSERT INTO customers (customer_id, name, email, phone, address, registration_date) VALUES (?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, "C001");
            stmt.setString(2, "John Doe");
            stmt.setString(3, "john.doe@example.com");
            stmt.setString(4, "555-123-4567");
            stmt.setString(5, "123 Main St, New York, NY");
            stmt.setString(6, LocalDateTime.now().minusDays(30).toString());
            stmt.addBatch();
            
            stmt.setString(1, "C002");
            stmt.setString(2, "Jane Smith");
            stmt.setString(3, "jane.smith@example.com");
            stmt.setString(4, "555-987-6543");
            stmt.setString(5, "456 Oak Ave, San Francisco, CA");
            stmt.setString(6, LocalDateTime.now().minusDays(25).toString());
            stmt.addBatch();
            
            stmt.setString(1, "C003");
            stmt.setString(2, "Bob Johnson");
            stmt.setString(3, "bob.johnson@example.com");
            stmt.setString(4, "555-456-7890");
            stmt.setString(5, "789 Pine St, Chicago, IL");
            stmt.setString(6, LocalDateTime.now().minusDays(20).toString());
            stmt.addBatch();
            
            stmt.setString(1, "C004");
            stmt.setString(2, "Alice Brown");
            stmt.setString(3, "alice.brown@example.com");
            stmt.setString(4, "555-789-0123");
            stmt.setString(5, "321 Elm St, Boston, MA");
            stmt.setString(6, LocalDateTime.now().minusDays(15).toString());
            stmt.addBatch();
            
            stmt.setString(1, "C005");
            stmt.setString(2, "Charlie Wilson");
            stmt.setString(3, "charlie.wilson@example.com");
            stmt.setString(4, "555-321-6547");
            stmt.setString(5, "654 Maple Ave, Seattle, WA");
            stmt.setString(6, LocalDateTime.now().minusDays(10).toString());
            stmt.addBatch();
            
            stmt.executeBatch();
        }
    }

    /**
     * Populates the products table with sample data.
     */
    protected void populateProducts() throws SQLException {
        String sql = "INSERT INTO products (product_id, name, category, price, inventory, last_updated) VALUES (?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, "P001");
            stmt.setString(2, "Laptop");
            stmt.setString(3, "Electronics");
            stmt.setDouble(4, 1299.99);
            stmt.setInt(5, 50);
            stmt.setString(6, LocalDateTime.now().minusDays(5).toString());
            stmt.addBatch();
            
            stmt.setString(1, "P002");
            stmt.setString(2, "Smartphone");
            stmt.setString(3, "Electronics");
            stmt.setDouble(4, 899.99);
            stmt.setInt(5, 100);
            stmt.setString(6, LocalDateTime.now().minusDays(4).toString());
            stmt.addBatch();
            
            stmt.setString(1, "P003");
            stmt.setString(2, "Coffee Maker");
            stmt.setString(3, "Appliances");
            stmt.setDouble(4, 79.99);
            stmt.setInt(5, 30);
            stmt.setString(6, LocalDateTime.now().minusDays(3).toString());
            stmt.addBatch();
            
            stmt.setString(1, "P004");
            stmt.setString(2, "Running Shoes");
            stmt.setString(3, "Clothing");
            stmt.setDouble(4, 129.99);
            stmt.setInt(5, 75);
            stmt.setString(6, LocalDateTime.now().minusDays(2).toString());
            stmt.addBatch();
            
            stmt.setString(1, "P005");
            stmt.setString(2, "Desk Chair");
            stmt.setString(3, "Furniture");
            stmt.setDouble(4, 199.99);
            stmt.setInt(5, 25);
            stmt.setString(6, LocalDateTime.now().minusDays(1).toString());
            stmt.addBatch();
            
            stmt.executeBatch();
        }
    }

    /**
     * Populates the transactions table with sample data.
     */
    protected void populateTransactions() throws SQLException {
        String sql = "INSERT INTO transactions (transaction_id, customer_id, product_id, transaction_date, amount, payment_method) VALUES (?, ?, ?, ?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, "T001");
            stmt.setString(2, "C001");
            stmt.setString(3, "P001");
            stmt.setString(4, LocalDateTime.now().minusDays(20).toString());
            stmt.setDouble(5, 1299.99);
            stmt.setString(6, "Credit Card");
            stmt.addBatch();
            
            stmt.setString(1, "T002");
            stmt.setString(2, "C002");
            stmt.setString(3, "P002");
            stmt.setString(4, LocalDateTime.now().minusDays(18).toString());
            stmt.setDouble(5, 899.99);
            stmt.setString(6, "PayPal");
            stmt.addBatch();
            
            stmt.setString(1, "T003");
            stmt.setString(2, "C003");
            stmt.setString(3, "P003");
            stmt.setString(4, LocalDateTime.now().minusDays(15).toString());
            stmt.setDouble(5, 79.99);
            stmt.setString(6, "Debit Card");
            stmt.addBatch();
            
            stmt.setString(1, "T004");
            stmt.setString(2, "C004");
            stmt.setString(3, "P004");
            stmt.setString(4, LocalDateTime.now().minusDays(10).toString());
            stmt.setDouble(5, 129.99);
            stmt.setString(6, "Credit Card");
            stmt.addBatch();
            
            stmt.setString(1, "T005");
            stmt.setString(2, "C005");
            stmt.setString(3, "P005");
            stmt.setString(4, LocalDateTime.now().minusDays(5).toString());
            stmt.setDouble(5, 199.99);
            stmt.setString(6, "Bank Transfer");
            stmt.addBatch();
            
            stmt.setString(1, "T006");
            stmt.setString(2, "C001");
            stmt.setString(3, "P002");
            stmt.setString(4, LocalDateTime.now().minusDays(3).toString());
            stmt.setDouble(5, 899.99);
            stmt.setString(6, "Credit Card");
            stmt.addBatch();
            
            stmt.setString(1, "T007");
            stmt.setString(2, "C002");
            stmt.setString(3, "P003");
            stmt.setString(4, LocalDateTime.now().minusDays(2).toString());
            stmt.setDouble(5, 79.99);
            stmt.setString(6, "PayPal");
            stmt.addBatch();
            
            stmt.setString(1, "T008");
            stmt.setString(2, "C003");
            stmt.setString(3, "P004");
            stmt.setString(4, LocalDateTime.now().minusDays(1).toString());
            stmt.setDouble(5, 129.99);
            stmt.setString(6, "Debit Card");
            stmt.addBatch();
            
            stmt.executeBatch();
        }
    }

    /**
     * Gets a connection to the database.
     */
    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
}
