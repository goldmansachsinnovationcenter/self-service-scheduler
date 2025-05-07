package com.gs.datalakehouse.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import com.gs.datalakehouse.core.util.DummyDataPopulator;
import java.util.Arrays;

/**
 * Main application class for the Data Lakehouse API.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.gs.datalakehouse"})
public class DataLakehouseApplication {

    public static void main(String[] args) {
        boolean isStubMode = Arrays.stream(args)
                .anyMatch(arg -> arg.contains("stub-hdfs"));
        
        if (isStubMode) {
            System.out.println("Starting in STUB HDFS mode");
            System.setProperty("spring.profiles.active", "stub-hdfs");
        }
        
        // Load dummy data before starting the application
        try {
            System.out.println("Loading dummy data...");
            DummyDataPopulator populator = new DummyDataPopulator(isStubMode ? "stub-hdfs" : "");
            try {
                populator.populateDummyData();
                System.out.println("Dummy data loaded successfully!");
            } catch (Exception e) {
                System.out.println("Note: Could not load dummy data. This is expected if Trino is not running.");
                System.out.println("The application will start without dummy data. Error: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Error initializing dummy data loader: " + e.getMessage());
        }
        
        // Start the Spring Boot application
        SpringApplication.run(DataLakehouseApplication.class, args);
    }
    
    @Bean
    public DummyDataPopulator dummyDataPopulator() {
        return new DummyDataPopulator(System.getProperty("spring.profiles.active", ""));
    }
}
