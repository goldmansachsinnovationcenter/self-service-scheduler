package com.gs.datalakehouse.api.controller;

import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.TableService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Tables", description = "API for managing Iceberg tables in the data lakehouse")
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
    @Operation(
        summary = "Create a new table",
        description = "Creates a new Iceberg table in the data lakehouse",
        tags = {"Tables"}
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "201", 
            description = "Table created successfully",
            content = @Content(mediaType = "application/json", schema = @Schema(implementation = Table.class))
        )
    })
    @PostMapping
    public ResponseEntity<Table> createTable(
            @Parameter(description = "Table definition", required = true)
            @Valid @RequestBody Table table) {
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
    @Operation(
        summary = "Get a table",
        description = "Retrieves a table by database and name",
        tags = {"Tables"}
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200", 
            description = "Table found",
            content = @Content(mediaType = "application/json", schema = @Schema(implementation = Table.class))
        ),
        @ApiResponse(
            responseCode = "404", 
            description = "Table not found",
            content = @Content
        )
    })
    @GetMapping("/{database}/{name}")
    public ResponseEntity<Table> getTable(
            @Parameter(description = "Database name", required = true) @PathVariable String database, 
            @Parameter(description = "Table name", required = true) @PathVariable String name) {
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
    @Operation(
        summary = "List tables",
        description = "Lists all tables in a specified database",
        tags = {"Tables"}
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200", 
            description = "Tables retrieved successfully",
            content = @Content(mediaType = "application/json")
        )
    })
    @GetMapping("/{database}")
    public ResponseEntity<List<Table>> listTables(
            @Parameter(description = "Database name", required = true) @PathVariable String database) {
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
    @Operation(
        summary = "Update a table",
        description = "Updates an existing table in the data lakehouse",
        tags = {"Tables"}
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200", 
            description = "Table updated successfully",
            content = @Content(mediaType = "application/json", schema = @Schema(implementation = Table.class))
        ),
        @ApiResponse(
            responseCode = "400", 
            description = "Invalid request - database or name mismatch",
            content = @Content
        ),
        @ApiResponse(
            responseCode = "404", 
            description = "Table not found",
            content = @Content
        )
    })
    @PutMapping("/{database}/{name}")
    public ResponseEntity<Table> updateTable(
            @Parameter(description = "Database name", required = true) @PathVariable String database, 
            @Parameter(description = "Table name", required = true) @PathVariable String name,
            @Parameter(description = "Updated table definition", required = true) @Valid @RequestBody Table table) {
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
    @Operation(
        summary = "Delete a table",
        description = "Deletes a table from the data lakehouse",
        tags = {"Tables"}
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "204", 
            description = "Table deleted successfully",
            content = @Content
        ),
        @ApiResponse(
            responseCode = "404", 
            description = "Table not found",
            content = @Content
        )
    })
    @DeleteMapping("/{database}/{name}")
    public ResponseEntity<Void> deleteTable(
            @Parameter(description = "Database name", required = true) @PathVariable String database, 
            @Parameter(description = "Table name", required = true) @PathVariable String name) {
        boolean deleted = tableService.deleteTable(database, name);
        return deleted ? new ResponseEntity<>(HttpStatus.NO_CONTENT) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
}
