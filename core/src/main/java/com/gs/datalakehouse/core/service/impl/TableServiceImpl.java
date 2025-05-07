package com.gs.datalakehouse.core.service.impl;

import com.gs.datalakehouse.core.model.Table;
import com.gs.datalakehouse.core.service.TableService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of the TableService interface for managing Iceberg tables.
 * This implementation uses an in-memory store for demonstration purposes.
 */
@Service
public class TableServiceImpl implements TableService {

    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    @Override
    public Table createTable(Table table) {
        String key = getTableKey(table.getDatabase(), table.getName());
        tables.put(key, table);
        return table;
    }

    @Override
    public Optional<Table> getTable(String database, String name) {
        String key = getTableKey(database, name);
        return Optional.ofNullable(tables.get(key));
    }

    @Override
    public List<Table> listTables(String database) {
        List<Table> result = new ArrayList<>();
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getKey().startsWith(database + ":")) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    @Override
    public Table updateTable(Table table) {
        String key = getTableKey(table.getDatabase(), table.getName());
        tables.put(key, table);
        return table;
    }

    @Override
    public boolean deleteTable(String database, String name) {
        String key = getTableKey(database, name);
        return tables.remove(key) != null;
    }

    private String getTableKey(String database, String name) {
        return database + ":" + name;
    }
}
