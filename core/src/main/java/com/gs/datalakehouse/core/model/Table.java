package com.gs.datalakehouse.core.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an Iceberg table in the data lakehouse.
 */
public class Table implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private String database;
    private String location;
    private List<Column> columns;
    private List<String> partitionBy;
    private Map<String, String> properties;
    private String format;
    private String description;

    public Table() {
    }

    public Table(String name, String database, String location, List<Column> columns,
                List<String> partitionBy, Map<String, String> properties, String format, String description) {
        this.name = name;
        this.database = database;
        this.location = location;
        this.columns = columns;
        this.partitionBy = partitionBy;
        this.properties = properties;
        this.format = format;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<String> getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(List<String> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return Objects.equals(name, table.name) &&
                Objects.equals(database, table.database);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, database);
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", database='" + database + '\'' +
                ", location='" + location + '\'' +
                ", columns=" + columns +
                ", partitionBy=" + partitionBy +
                ", properties=" + properties +
                ", format='" + format + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
