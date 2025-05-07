package com.gs.datalakehouse.core.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    void testConstructorAndGetters() {
        Column column = new Column("id", "string", false, "Primary key");
        
        assertEquals("id", column.getName());
        assertEquals("string", column.getType());
        assertFalse(column.isNullable());
        assertEquals("Primary key", column.getComment());
    }
    
    @Test
    void testSetters() {
        Column column = new Column();
        
        column.setName("timestamp");
        column.setType("timestamp");
        column.setNullable(true);
        column.setComment("Event timestamp");
        
        assertEquals("timestamp", column.getName());
        assertEquals("timestamp", column.getType());
        assertTrue(column.isNullable());
        assertEquals("Event timestamp", column.getComment());
    }
    
    @Test
    void testEquals() {
        Column column1 = new Column("id", "string", false, "Primary key");
        Column column2 = new Column("id", "integer", true, "Different comment");
        Column column3 = new Column("name", "string", false, "Primary key");
        
        assertEquals(column1, column2);
        assertNotEquals(column1, column3);
        assertNotEquals(column2, column3);
        
        assertNotEquals(column1, null);
        assertNotEquals(column1, "not a column");
    }
    
    @Test
    void testHashCode() {
        Column column1 = new Column("id", "string", false, "Primary key");
        Column column2 = new Column("id", "integer", true, "Different comment");
        
        assertEquals(column1.hashCode(), column2.hashCode());
    }
    
    @Test
    void testToString() {
        Column column = new Column("id", "string", false, "Primary key");
        String toString = column.toString();
        
        assertTrue(toString.contains("id"));
        assertTrue(toString.contains("string"));
        assertTrue(toString.contains("false"));
        assertTrue(toString.contains("Primary key"));
    }
}
