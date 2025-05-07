package com.gs.datalakehouse.core.service.impl;

import com.gs.datalakehouse.core.model.DataSource;
import com.gs.datalakehouse.core.service.DataSourceService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of the DataSourceService interface for managing data sources.
 * This implementation uses an in-memory store for demonstration purposes.
 */
@Service
public class DataSourceServiceImpl implements DataSourceService {

    private final Map<String, DataSource> dataSources = new ConcurrentHashMap<>();

    @Override
    public DataSource createDataSource(DataSource dataSource) {
        dataSources.put(dataSource.getName(), dataSource);
        return dataSource;
    }

    @Override
    public Optional<DataSource> getDataSource(String name) {
        return Optional.ofNullable(dataSources.get(name));
    }

    @Override
    public List<DataSource> listDataSources() {
        return new ArrayList<>(dataSources.values());
    }

    @Override
    public DataSource updateDataSource(DataSource dataSource) {
        dataSources.put(dataSource.getName(), dataSource);
        return dataSource;
    }

    @Override
    public boolean deleteDataSource(String name) {
        return dataSources.remove(name) != null;
    }
}
