package com.gs.datalakehouse.core.service.impl;

import com.gs.datalakehouse.core.service.FileSystemService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Implementation of FileSystemService that uses real HDFS.
 */
@Service
@Profile("!stub-hdfs")
public class HdfsFileSystemServiceImpl implements FileSystemService {

    @Value("${hdfs.uri}")
    private String hdfsUri;
    
    @Value("${hdfs.user}")
    private String hdfsUser;
    
    private FileSystem fileSystem;
    
    @PostConstruct
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        System.setProperty("HADOOP_USER_NAME", hdfsUser);
        fileSystem = FileSystem.get(conf);
    }
    
    @Override
    public boolean exists(String path) throws IOException {
        return fileSystem.exists(new Path(path));
    }
    
    @Override
    public boolean mkdirs(String path) throws IOException {
        return fileSystem.mkdirs(new Path(path));
    }
    
    @Override
    public InputStream open(String path) throws IOException {
        return fileSystem.open(new Path(path));
    }
    
    @Override
    public OutputStream create(String path) throws IOException {
        return fileSystem.create(new Path(path));
    }
    
    @Override
    public boolean delete(String path, boolean recursive) throws IOException {
        return fileSystem.delete(new Path(path), recursive);
    }
    
    @Override
    public String[] listFiles(String path) throws IOException {
        return Arrays.stream(fileSystem.listStatus(new Path(path)))
                .map(status -> status.getPath().toString())
                .toArray(String[]::new);
    }
}
