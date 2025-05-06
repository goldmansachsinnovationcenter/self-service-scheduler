package com.gs.datalakehouse.api.controller;

import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

/**
 * REST controller for managing Iceberg tables.
 */
@RestController
@RequestMapping("/api/tables")
public class TableController {

    private final TableService tableService;

    @Autowired
    public TableController(TableService tableService) {
        this.tableService = tableService;
    }

    /**
     * Creates a new table.
     *
     * @param table the table to create
     * @return the created table
     */
    @PostMapping
    public ResponseEntity<Table> createTable(@Valid @RequestBody Table table) {
        Table createdTable = tableService.createTable(table);
        return new ResponseEntity<>(createdTable, HttpStatus.CREATED);
    }

    /**
     * Gets a table by database and name.
     *
     * @param database the database name
     * @param name the table name
     * @return the table if found
     */
    @GetMapping("/{database}/{name}")
    public ResponseEntity<Table> getTable(@PathVariable String database, @PathVariable String name) {
        return tableService.getTable(database, name)
                .map(table -> new ResponseEntity<>(table, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * Lists all tables in a database.
     *
     * @param database the database name
     * @return the list of tables
     */
    @GetMapping("/{database}")
    public ResponseEntity<List<Table>> listTables(@PathVariable String database) {
        List<Table> tables = tableService.listTables(database);
        return new ResponseEntity<>(tables, HttpStatus.OK);
    }

    /**
     * Updates an existing table.
     *
     * @param database the database name
     * @param name the table name
     * @param table the updated table
     * @return the updated table
     */
    @PutMapping("/{database}/{name}")
    public ResponseEntity<Table> updateTable(@PathVariable String database, @PathVariable String name,
                                            @Valid @RequestBody Table table) {
        if (!database.equals(table.getDatabase()) || !name.equals(table.getName())) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
        Table updatedTable = tableService.updateTable(table);
        return new ResponseEntity<>(updatedTable, HttpStatus.OK);
    }

    /**
     * Deletes a table.
     *
     * @param database the database name
     * @param name the table name
     * @return no content if successful
     */
    @DeleteMapping("/{database}/{name}")
    public ResponseEntity<Void> deleteTable(@PathVariable String database, @PathVariable String name) {
        boolean deleted = tableService.deleteTable(database, name);
        return deleted ? new ResponseEntity<>(HttpStatus.NO_CONTENT) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
}
