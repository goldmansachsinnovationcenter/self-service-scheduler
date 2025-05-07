package com.gs.datalakehouse.api;

import com.gs.datalakehouse.core.util.DummyDataPopulator;

/**
 * Standalone application to load dummy data.
 * This class can be used to populate the database with sample data
 * without starting the full Spring Boot application.
 */
public class DummyDataLoader {

    public static void main(String[] args) {
        System.out.println("Starting dummy data loader...");
        
        try {
            System.out.println("Initializing DummyDataPopulator...");
            DummyDataPopulator populator = new DummyDataPopulator();
            
            System.out.println("Loading dummy data...");
            populator.populateDummyData();
            
            System.out.println("Dummy data loaded successfully!");
        } catch (Exception e) {
            System.err.println("Error loading dummy data: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
