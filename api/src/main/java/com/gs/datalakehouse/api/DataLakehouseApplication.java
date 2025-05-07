package com.gs.datalakehouse.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import com.gs.datalakehouse.core.util.DummyDataPopulator;

/**
 * Main application class for the Data Lakehouse API.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.gs.datalakehouse"})
public class DataLakehouseApplication {

    public static void main(String[] args) {
        // Load dummy data before starting the application
        try {
            System.out.println("Loading dummy data...");
            DummyDataPopulator populator = new DummyDataPopulator();
            populator.populateDummyData();
            System.out.println("Dummy data loaded successfully!");
        } catch (Exception e) {
            System.err.println("Error loading dummy data: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Start the Spring Boot application
        SpringApplication.run(DataLakehouseApplication.class, args);
    }
    
    @Bean
    public DummyDataPopulator dummyDataPopulator() {
        return new DummyDataPopulator();
    }
}
