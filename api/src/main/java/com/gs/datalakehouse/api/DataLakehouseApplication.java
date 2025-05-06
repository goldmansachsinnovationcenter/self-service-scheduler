package com.gs.datalakehouse.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Main application class for the Data Lakehouse API.
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.gs.datalakehouse"})
public class DataLakehouseApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataLakehouseApplication.class, args);
    }
}
