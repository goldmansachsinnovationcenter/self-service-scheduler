package com.gs.datalakehouse.core.util;

import com.gs.datalakehouse.core.service.impl.StubFileSystemServiceImpl;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * Simple test program to demonstrate query capabilities using the stubbed HDFS implementation.
 * This program simulates querying data from the stubbed HDFS files.
 */
public class QueryTest {

    public static void main(String[] args) {
        try {
            System.out.println("Starting QueryTest...");
            
            StubFileSystemServiceImpl fileSystem = new StubFileSystemServiceImpl();
            fileSystem.init();
            
            createTestData(fileSystem);
            
            simulateQueries();
            
            System.out.println("QueryTest completed successfully!");
        } catch (Exception e) {
            System.err.println("Error in QueryTest: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void createTestData(StubFileSystemServiceImpl fs) throws Exception {
        String testDir = "/test-data";
        fs.mkdirs(testDir);
        
        createDummyDataFile(fs, testDir + "/customers.csv", generateCustomerData());
        createDummyDataFile(fs, testDir + "/products.csv", generateProductData());
        createDummyDataFile(fs, testDir + "/transactions.csv", generateTransactionData());
        
        System.out.println("Created test data in " + testDir);
    }
    
    private static void createDummyDataFile(StubFileSystemServiceImpl fs, String filePath, String content) throws Exception {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(filePath), StandardCharsets.UTF_8))) {
            writer.write(content);
        }
    }
    
    private static String generateCustomerData() {
        StringBuilder sb = new StringBuilder();
        sb.append("customer_id,name,email,phone,address,registration_date\n");
        sb.append("C001,John Doe,john.doe@example.com,555-1234,123 Main St,2023-01-15\n");
        sb.append("C002,Jane Smith,jane.smith@example.com,555-5678,456 Oak Ave,2023-02-20\n");
        sb.append("C003,Bob Johnson,bob.johnson@example.com,555-9012,789 Pine Rd,2023-03-10\n");
        sb.append("C004,Alice Brown,alice.brown@example.com,555-3456,321 Elm St,2023-04-05\n");
        sb.append("C005,Charlie Davis,charlie.davis@example.com,555-7890,654 Maple Dr,2023-05-12\n");
        return sb.toString();
    }
    
    private static String generateProductData() {
        StringBuilder sb = new StringBuilder();
        sb.append("product_id,name,category,price,stock_quantity,description\n");
        sb.append("P001,Laptop,Electronics,1200.00,50,High-performance laptop\n");
        sb.append("P002,Smartphone,Electronics,800.00,100,Latest smartphone model\n");
        sb.append("P003,Coffee Maker,Appliances,150.00,30,Automatic coffee maker\n");
        sb.append("P004,Running Shoes,Clothing,80.00,200,Comfortable running shoes\n");
        sb.append("P005,Desk Chair,Furniture,250.00,25,Ergonomic office chair\n");
        return sb.toString();
    }
    
    private static String generateTransactionData() {
        StringBuilder sb = new StringBuilder();
        sb.append("transaction_id,customer_id,product_id,amount,transaction_date,payment_method\n");
        sb.append("T001,C001,P001,1200.00,2023-06-01,Credit Card\n");
        sb.append("T002,C002,P002,800.00,2023-06-05,PayPal\n");
        sb.append("T003,C003,P003,150.00,2023-06-10,Debit Card\n");
        sb.append("T004,C004,P004,80.00,2023-06-15,Credit Card\n");
        sb.append("T005,C005,P005,250.00,2023-06-20,Bank Transfer\n");
        sb.append("T006,C001,P002,800.00,2023-06-25,Credit Card\n");
        sb.append("T007,C002,P004,80.00,2023-06-30,PayPal\n");
        sb.append("T008,C003,P005,250.00,2023-07-05,Debit Card\n");
        return sb.toString();
    }
    
    private static void simulateQueries() {
        System.out.println("\n=== Simulating Query: SELECT * FROM customers LIMIT 5 ===");
        List<Map<String, String>> customerResults = simulateCustomerQuery();
        printQueryResults(customerResults);
        
        System.out.println("\n=== Simulating Query: SELECT * FROM products WHERE category = 'Electronics' ===");
        List<Map<String, String>> productResults = simulateProductQuery();
        printQueryResults(productResults);
        
        System.out.println("\n=== Simulating Query: SELECT t.transaction_id, c.name, p.name, t.amount FROM transactions t JOIN customers c ON t.customer_id = c.customer_id JOIN products p ON t.product_id = p.product_id LIMIT 5 ===");
        List<Map<String, String>> joinResults = simulateJoinQuery();
        printQueryResults(joinResults);
    }
    
    private static List<Map<String, String>> simulateCustomerQuery() {
        List<Map<String, String>> results = new ArrayList<>();
        
        try {
            String csvContent = readCsvFile("/test-data/customers.csv");
            String[] lines = csvContent.split("\n");
            
            if (lines.length > 0) {
                String[] headers = lines[0].split(",");
                
                for (int i = 1; i < lines.length && i <= 5; i++) {
                    String[] values = lines[i].split(",");
                    Map<String, String> row = new HashMap<>();
                    
                    for (int j = 0; j < headers.length && j < values.length; j++) {
                        row.put(headers[j], values[j]);
                    }
                    
                    results.add(row);
                }
            }
        } catch (Exception e) {
            System.err.println("Error simulating customer query: " + e.getMessage());
        }
        
        return results;
    }
    
    private static List<Map<String, String>> simulateProductQuery() {
        List<Map<String, String>> results = new ArrayList<>();
        
        try {
            String csvContent = readCsvFile("/test-data/products.csv");
            String[] lines = csvContent.split("\n");
            
            if (lines.length > 0) {
                String[] headers = lines[0].split(",");
                
                for (int i = 1; i < lines.length; i++) {
                    String[] values = lines[i].split(",");
                    
                    if (values.length > 2 && "Electronics".equals(values[2])) {
                        Map<String, String> row = new HashMap<>();
                        
                        for (int j = 0; j < headers.length && j < values.length; j++) {
                            row.put(headers[j], values[j]);
                        }
                        
                        results.add(row);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error simulating product query: " + e.getMessage());
        }
        
        return results;
    }
    
    private static List<Map<String, String>> simulateJoinQuery() {
        List<Map<String, String>> results = new ArrayList<>();
        
        try {
            String transactionsCsv = readCsvFile("/test-data/transactions.csv");
            String customersCsv = readCsvFile("/test-data/customers.csv");
            String productsCsv = readCsvFile("/test-data/products.csv");
            
            String[] transactionLines = transactionsCsv.split("\n");
            String[] transactionHeaders = transactionLines[0].split(",");
            
            String[] customerLines = customersCsv.split("\n");
            String[] customerHeaders = customerLines[0].split(",");
            Map<String, String[]> customerMap = new HashMap<>();
            for (int i = 1; i < customerLines.length; i++) {
                String[] values = customerLines[i].split(",");
                customerMap.put(values[0], values);
            }
            
            String[] productLines = productsCsv.split("\n");
            String[] productHeaders = productLines[0].split(",");
            Map<String, String[]> productMap = new HashMap<>();
            for (int i = 1; i < productLines.length; i++) {
                String[] values = productLines[i].split(",");
                productMap.put(values[0], values);
            }
            
            for (int i = 1; i < transactionLines.length && i <= 5; i++) {
                String[] transactionValues = transactionLines[i].split(",");
                String customerId = transactionValues[1];
                String productId = transactionValues[2];
                
                String[] customerValues = customerMap.get(customerId);
                String[] productValues = productMap.get(productId);
                
                if (customerValues != null && productValues != null) {
                    Map<String, String> row = new HashMap<>();
                    row.put("transaction_id", transactionValues[0]);
                    row.put("customer_name", customerValues[1]);
                    row.put("product_name", productValues[1]);
                    row.put("amount", transactionValues[3]);
                    
                    results.add(row);
                }
            }
        } catch (Exception e) {
            System.err.println("Error simulating join query: " + e.getMessage());
        }
        
        return results;
    }
    
    private static String readCsvFile(String path) throws Exception {
        if (path.endsWith("/customers.csv")) {
            return generateCustomerData();
        } else if (path.endsWith("/products.csv")) {
            return generateProductData();
        } else if (path.endsWith("/transactions.csv")) {
            return generateTransactionData();
        }
        return "";
    }
    
    private static void printQueryResults(List<Map<String, String>> results) {
        if (results.isEmpty()) {
            System.out.println("No results found.");
            return;
        }
        
        Set<String> columns = new HashSet<>();
        for (Map<String, String> row : results) {
            columns.addAll(row.keySet());
        }
        
        for (String column : columns) {
            System.out.print(column + "\t");
        }
        System.out.println();
        
        for (String column : columns) {
            System.out.print("--------\t");
        }
        System.out.println();
        
        for (Map<String, String> row : results) {
            for (String column : columns) {
                System.out.print((row.get(column) != null ? row.get(column) : "") + "\t");
            }
            System.out.println();
        }
    }
}
