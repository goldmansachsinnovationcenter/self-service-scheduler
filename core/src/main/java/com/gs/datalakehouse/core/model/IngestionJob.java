package com.gs.datalakehouse.core.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a data ingestion job from a source to a target table.
 */
public class IngestionJob implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private String sourceRef;
    private String targetTableRef;
    private String transformationClass;
    private Map<String, String> properties;
    private String schedule;
    private String description;

    public IngestionJob() {
    }

    public IngestionJob(String name, String sourceRef, String targetTableRef, 
                        String transformationClass, Map<String, String> properties, 
                        String schedule, String description) {
        this.name = name;
        this.sourceRef = sourceRef;
        this.targetTableRef = targetTableRef;
        this.transformationClass = transformationClass;
        this.properties = properties;
        this.schedule = schedule;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSourceRef() {
        return sourceRef;
    }

    public void setSourceRef(String sourceRef) {
        this.sourceRef = sourceRef;
    }

    public String getTargetTableRef() {
        return targetTableRef;
    }

    public void setTargetTableRef(String targetTableRef) {
        this.targetTableRef = targetTableRef;
    }

    public String getTransformationClass() {
        return transformationClass;
    }

    public void setTransformationClass(String transformationClass) {
        this.transformationClass = transformationClass;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
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
        IngestionJob that = (IngestionJob) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "IngestionJob{" +
                "name='" + name + '\'' +
                ", sourceRef='" + sourceRef + '\'' +
                ", targetTableRef='" + targetTableRef + '\'' +
                ", transformationClass='" + transformationClass + '\'' +
                ", properties=" + properties +
                ", schedule='" + schedule + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
