package com.gs.datalakehouse.core.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a data source for ingestion into the data lakehouse.
 */
public class DataSource implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum SourceType {
        KAFKA,
        FILE,
        DATABASE,
        API
    }

    private String name;
    private SourceType type;
    private Map<String, String> properties;
    private String description;

    public DataSource() {
    }

    public DataSource(String name, SourceType type, Map<String, String> properties, String description) {
        this.name = name;
        this.type = type;
        this.properties = properties;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SourceType getType() {
        return type;
    }

    public void setType(SourceType type) {
        this.type = type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
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
        DataSource that = (DataSource) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "DataSource{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", properties=" + properties +
                ", description='" + description + '\'' +
                '}';
    }
}
