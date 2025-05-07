package com.gs.datalakehouse.core.service;

import com.gs.datalakehouse.core.model.DataSource;

import java.util.List;
import java.util.Optional;

/**
 * Service interface for managing data sources.
 */
public interface DataSourceService {

    /**
     * Creates a new data source.
     *
     * @param dataSource the data source to create
     * @return the created data source
     */
    DataSource createDataSource(DataSource dataSource);

    /**
     * Gets a data source by its name.
     *
     * @param name the data source name
     * @return the data source if found
     */
    Optional<DataSource> getDataSource(String name);

    /**
     * Lists all data sources.
     *
     * @return the list of data sources
     */
    List<DataSource> listDataSources();

    /**
     * Updates an existing data source.
     *
     * @param dataSource the data source to update
     * @return the updated data source
     */
    DataSource updateDataSource(DataSource dataSource);

    /**
     * Deletes a data source.
     *
     * @param name the data source name
     * @return true if the data source was deleted, false otherwise
     */
    boolean deleteDataSource(String name);
}
