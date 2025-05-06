package com.gs.datalakehouse.api.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST controller for executing SQL queries against Trino.
 */
@RestController
@RequestMapping("/api/query")
public class QueryController {

    @Value("${trino.host}")
    private String trinoHost;

    @Value("${trino.port}")
    private int trinoPort;

    @Value("${trino.user}")
    private String trinoUser;

    @Value("${trino.catalog}")
    private String trinoCatalog;

    @Value("${trino.schema}")
    private String trinoSchema;

    /**
     * Executes a SQL query against Trino.
     *
     * @param queryRequest the query request containing the SQL
     * @return the query results
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> executeQuery(@Valid @RequestBody QueryRequest queryRequest) {
        String sql = queryRequest.getSql();
        
        try {
            String url = String.format("jdbc:trino://%s:%d/%s/%s", trinoHost, trinoPort, trinoCatalog, trinoSchema);
            Connection connection = DriverManager.getConnection(url, trinoUser, null);
            
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                List<String> columnNames = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columnNames.add(metaData.getColumnName(i));
                }
                
                List<Map<String, Object>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnName(i), resultSet.getObject(i));
                    }
                    rows.add(row);
                }
                
                Map<String, Object> response = new HashMap<>();
                response.put("columns", columnNames);
                response.put("rows", rows);
                
                return new ResponseEntity<>(response, HttpStatus.OK);
            }
        } catch (SQLException e) {
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", e.getMessage());
            return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Request class for SQL queries.
     */
    public static class QueryRequest {
        private String sql;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }
    }
}
