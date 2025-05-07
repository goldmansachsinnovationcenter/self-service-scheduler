package com.gs.datalakehouse.core.service.impl;

import com.gs.datalakehouse.core.service.FileSystemService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Stubbed implementation of FileSystemService that uses local file system.
 */
@Service
@Profile("stub-hdfs")
public class StubFileSystemServiceImpl implements FileSystemService {

    private static final String BASE_DIR = "/tmp/stub-hdfs";
    
    @PostConstruct
    public void init() throws IOException {
        Path basePath = Paths.get(BASE_DIR);
        if (!Files.exists(basePath)) {
            Files.createDirectories(basePath);
        }
        
        Path warehousePath = Paths.get(BASE_DIR, "warehouse");
        if (!Files.exists(warehousePath)) {
            Files.createDirectories(warehousePath);
        }
        
        System.out.println("Initialized stubbed HDFS at " + BASE_DIR);
    }
    
    @Override
    public boolean exists(String path) throws IOException {
        return Files.exists(convertPath(path));
    }
    
    @Override
    public boolean mkdirs(String path) throws IOException {
        Files.createDirectories(convertPath(path));
        return true;
    }
    
    @Override
    public InputStream open(String path) throws IOException {
        return Files.newInputStream(convertPath(path));
    }
    
    @Override
    public OutputStream create(String path) throws IOException {
        Path localPath = convertPath(path);
        if (!Files.exists(localPath.getParent())) {
            Files.createDirectories(localPath.getParent());
        }
        return Files.newOutputStream(localPath);
    }
    
    @Override
    public boolean delete(String path, boolean recursive) throws IOException {
        Path localPath = convertPath(path);
        if (recursive && Files.isDirectory(localPath)) {
            try (Stream<Path> pathStream = Files.walk(localPath)) {
                pathStream.sorted((a, b) -> -a.compareTo(b)) // Reverse order to delete children first
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
            }
            return true;
        } else {
            Files.delete(localPath);
            return true;
        }
    }
    
    @Override
    public String[] listFiles(String path) throws IOException {
        Path localPath = convertPath(path);
        if (!Files.exists(localPath)) {
            return new String[0];
        }
        
        try (Stream<Path> pathStream = Files.list(localPath)) {
            return pathStream
                    .map(Path::toString)
                    .toArray(String[]::new);
        }
    }
    
    /**
     * Converts an HDFS path to a local file system path.
     *
     * @param hdfsPath the HDFS path
     * @return the corresponding local path
     */
    private Path convertPath(String hdfsPath) {
        String localPath = hdfsPath.replaceAll("^hdfs://[^/]+", "");
        if (!localPath.startsWith("/")) {
            localPath = "/" + localPath;
        }
        return Paths.get(BASE_DIR, localPath);
    }
}
