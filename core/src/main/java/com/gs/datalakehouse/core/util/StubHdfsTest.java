package com.gs.datalakehouse.core.util;

import com.gs.datalakehouse.core.service.FileSystemService;
import com.gs.datalakehouse.core.service.impl.StubFileSystemServiceImpl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * Simple test program to verify the stubbed HDFS implementation works.
 * This can be run directly without Spring Boot to test the core functionality.
 */
public class StubHdfsTest {

    public static void main(String[] args) {
        try {
            System.out.println("Starting StubHdfsTest...");
            
            StubFileSystemServiceImpl fileSystem = new StubFileSystemServiceImpl();
            fileSystem.init();
            
            testFileSystemOperations(fileSystem);
            
            System.out.println("StubHdfsTest completed successfully!");
        } catch (Exception e) {
            System.err.println("Error in StubHdfsTest: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testFileSystemOperations(FileSystemService fs) throws Exception {
        String testDir = "/test-data";
        fs.mkdirs(testDir);
        System.out.println("Created directory: " + testDir);
        
        String testFile = testDir + "/sample.txt";
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(testFile), StandardCharsets.UTF_8))) {
            writer.write("This is a test file created by StubHdfsTest.\n");
            writer.write("Line 1: Sample data for testing.\n");
            writer.write("Line 2: More sample data for testing.\n");
        }
        System.out.println("Created and wrote to file: " + testFile);
        
        System.out.println("Reading file content:");
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(testFile), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("  " + line);
            }
        }
        
        boolean exists = fs.exists(testFile);
        System.out.println("File exists: " + exists);
        
        String[] files = fs.listFiles(testDir);
        System.out.println("Files in directory " + testDir + ":");
        for (String file : files) {
            System.out.println("  " + file);
        }
        
        createDummyDataFile(fs, testDir + "/customers.csv", generateCustomerData());
        createDummyDataFile(fs, testDir + "/products.csv", generateProductData());
        createDummyDataFile(fs, testDir + "/transactions.csv", generateTransactionData());
        
        System.out.println("Created dummy data files in " + testDir);
    }
    
    private static void createDummyDataFile(FileSystemService fs, String filePath, String content) throws Exception {
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
}
