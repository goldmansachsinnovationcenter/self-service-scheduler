package com.gs.datalakehouse.core.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a column in an Iceberg table.
 */
public class Column implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private String type;
    private boolean nullable;
    private String comment;

    public Column() {
    }

    public Column(String name, String type, boolean nullable, String comment) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Objects.equals(name, column.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", nullable=" + nullable +
                ", comment='" + comment + '\'' +
                '}';
    }
}
