package com.gs.datalakehouse.core.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class StubFileSystemServiceImplTest {

    private StubFileSystemServiceImpl fileSystemService;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() throws Exception {
        fileSystemService = new StubFileSystemServiceImpl();
        
        Field field = StubFileSystemServiceImpl.class.getDeclaredField("BASE_DIR");
        field.setAccessible(true);
        field.set(null, tempDir.toString());
        
        fileSystemService.init();
    }
    
    @Test
    void testMkdirs() throws IOException {
        String testDir = "hdfs://localhost:9000/test/dir";
        assertTrue(fileSystemService.mkdirs(testDir));
        assertTrue(Files.exists(tempDir.resolve("test/dir")));
    }
    
    @Test
    void testCreateAndOpen() throws IOException {
        String testFile = "hdfs://localhost:9000/test/file.txt";
        String content = "Hello World";
        
        try (OutputStream os = fileSystemService.create(testFile)) {
            os.write(content.getBytes(StandardCharsets.UTF_8));
        }
        
        try (InputStream is = fileSystemService.open(testFile)) {
            byte[] buffer = new byte[100];
            int len = is.read(buffer);
            String readContent = new String(buffer, 0, len, StandardCharsets.UTF_8);
            assertEquals(content, readContent);
        }
    }
    
    @Test
    void testExists() throws IOException {
        String testFile = "hdfs://localhost:9000/test/exists.txt";
        
        assertFalse(fileSystemService.exists(testFile));
        
        try (OutputStream os = fileSystemService.create(testFile)) {
            os.write("test".getBytes(StandardCharsets.UTF_8));
        }
        
        assertTrue(fileSystemService.exists(testFile));
    }
    
    @Test
    void testDelete() throws IOException {
        String testFile = "hdfs://localhost:9000/test/delete.txt";
        
        try (OutputStream os = fileSystemService.create(testFile)) {
            os.write("test".getBytes(StandardCharsets.UTF_8));
        }
        
        assertTrue(fileSystemService.exists(testFile));
        
        assertTrue(fileSystemService.delete(testFile, false));
        
        assertFalse(fileSystemService.exists(testFile));
    }
    
    @Test
    void testListFiles() throws IOException {
        String testDir = "hdfs://localhost:9000/test/list";
        fileSystemService.mkdirs(testDir);
        
        try (OutputStream os = fileSystemService.create(testDir + "/file1.txt")) {
            os.write("test1".getBytes(StandardCharsets.UTF_8));
        }
        try (OutputStream os = fileSystemService.create(testDir + "/file2.txt")) {
            os.write("test2".getBytes(StandardCharsets.UTF_8));
        }
        
        String[] files = fileSystemService.listFiles(testDir);
        assertEquals(2, files.length);
    }
}
