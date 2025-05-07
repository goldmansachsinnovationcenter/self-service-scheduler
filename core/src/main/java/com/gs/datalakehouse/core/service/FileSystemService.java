package com.gs.datalakehouse.core.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Service interface for file system operations.
 * This abstraction allows for different implementations (real HDFS or stubbed).
 */
public interface FileSystemService {
    
    /**
     * Checks if a path exists.
     *
     * @param path the path to check
     * @return true if the path exists, false otherwise
     * @throws IOException if an I/O error occurs
     */
    boolean exists(String path) throws IOException;
    
    /**
     * Creates directories for the given path.
     *
     * @param path the path to create directories for
     * @return true if the directories were created, false otherwise
     * @throws IOException if an I/O error occurs
     */
    boolean mkdirs(String path) throws IOException;
    
    /**
     * Opens an input stream for the given path.
     *
     * @param path the path to open an input stream for
     * @return the input stream
     * @throws IOException if an I/O error occurs
     */
    InputStream open(String path) throws IOException;
    
    /**
     * Creates an output stream for the given path.
     *
     * @param path the path to create an output stream for
     * @return the output stream
     * @throws IOException if an I/O error occurs
     */
    OutputStream create(String path) throws IOException;
    
    /**
     * Deletes the given path.
     *
     * @param path the path to delete
     * @param recursive whether to delete recursively
     * @return true if the path was deleted, false otherwise
     * @throws IOException if an I/O error occurs
     */
    boolean delete(String path, boolean recursive) throws IOException;
    
    /**
     * Lists files in the given path.
     *
     * @param path the path to list files in
     * @return array of file paths
     * @throws IOException if an I/O error occurs
     */
    String[] listFiles(String path) throws IOException;
}
