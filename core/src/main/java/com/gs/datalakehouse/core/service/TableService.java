package com.gs.datalakehouse.core.service;

import com.gs.datalakehouse.core.model.Table;

import java.util.List;
import java.util.Optional;

/**
 * Service interface for managing Iceberg tables.
 */
public interface TableService {

    /**
     * Creates a new table in the data lakehouse.
     *
     * @param table the table to create
     * @return the created table
     */
    Table createTable(Table table);

    /**
     * Gets a table by its name and database.
     *
     * @param database the database name
     * @param name the table name
     * @return the table if found
     */
    Optional<Table> getTable(String database, String name);

    /**
     * Lists all tables in a database.
     *
     * @param database the database name
     * @return the list of tables
     */
    List<Table> listTables(String database);

    /**
     * Updates an existing table.
     *
     * @param table the table to update
     * @return the updated table
     */
    Table updateTable(Table table);

    /**
     * Deletes a table.
     *
     * @param database the database name
     * @param name the table name
     * @return true if the table was deleted, false otherwise
     */
    boolean deleteTable(String database, String name);
}
