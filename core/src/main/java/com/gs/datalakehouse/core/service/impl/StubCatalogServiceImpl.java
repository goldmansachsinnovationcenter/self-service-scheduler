package com.gs.datalakehouse.core.service.impl;

import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.FileSystemService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Stubbed implementation of catalog service that works with the stubbed file system.
 */
@Service
@Profile("stub-hdfs")
public class StubCatalogServiceImpl {

    private static final String CATALOG_DIR = "/tmp/stub-hdfs/catalog";
    
    @Autowired
    private FileSystemService fileSystemService;
    
    @PostConstruct
    public void init() throws IOException {
        Path catalogPath = Paths.get(CATALOG_DIR);
        if (!Files.exists(catalogPath)) {
            Files.createDirectories(catalogPath);
        }
        
        System.out.println("Initialized stubbed catalog at " + CATALOG_DIR);
    }
    
    /**
     * Registers a table in the stubbed catalog.
     *
     * @param table the table to register
     * @throws IOException if an I/O error occurs
     */
    public void registerTable(Table table) throws IOException {
        String tablePath = CATALOG_DIR + "/" + table.getDatabase() + "/" + table.getName();
        if (!fileSystemService.exists(tablePath)) {
            fileSystemService.mkdirs(tablePath);
        }
    }
}
