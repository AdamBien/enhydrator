package com.airhacks.enhydrator.in;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ColumnTest {

    @Test
    public void convertToString() {
        Column column = new Column(0, "name", 42);
        assertTrue(column.getValue() instanceof Integer);
        column.convertToString();
        assertTrue(column.getValue() instanceof String);
    }

    @Test
    public void convertToInteger() {
        Column column = new Column(0, "name", "42");
        column.convertToInteger();
        assertTrue(column.getValue() instanceof Integer);
    }

    @Test
    public void convertToDouble() {
        Column column = new Column(0, "name", "42.0");
        column.convertToDouble();
        assertTrue(column.getValue() instanceof Double);
    }

    @Test
    public void convertToTrueBoolean() {
        Column column = new Column(0, "flag", "true");
        column.convertToBoolean();
        assertTrue(column.getValue() instanceof Boolean);
        Boolean booleanValue = (Boolean) column.getValue();
        assertTrue(booleanValue);
    }

    @Test
    public void convertToFalseBoolean() {
        Column column = new Column(0, "flag", "false");
        column.convertToBoolean();
        assertTrue(column.getValue() instanceof Boolean);
        Boolean booleanValue = (Boolean) column.getValue();
        assertFalse(booleanValue);
    }

    @Test
    public void convertEmptyStringToDouble() {
        Column column = new Column(0, "double", "");
        column.convertToDouble();
        assertTrue(column.getValue() instanceof Double);
        Double value = (Double) column.getValue();
        assertThat(value, is(0.0));
    }

    @Test
    public void convertDMSToDouble() {
        Column column = new Column(0, "dms", "1.4.2");
        column.convertDMSToDouble();
        assertTrue(column.getValue() instanceof Double);
        Double value = (Double) column.getValue();
        assertThat(value, is(1.0672222222222223d));
    }

    @Test
    public void convertDMSToDoubleWithUnsufficientPrecision() {
        Column column = new Column(0, "dms", "0.0");
        column.convertDMSToDouble();
        assertTrue(column.getValue() instanceof Double);
        Double value = (Double) column.getValue();
        assertThat(value, is(0.0d));
    }

    @Test
    public void testIsNumber() {
        Column column = new Column(0, "number", 42);
        assertTrue(column.isNumber());

        Column nullColumn = new Column(0, "number");
        assertFalse(nullColumn.isNumber());
    }

    @Test
    public void testIsString() {
        Column column = new Column(0, "string", "aString");
        assertTrue(column.isString());

        Column nullColumn = new Column(0, "string");
        assertFalse(nullColumn.isString());
    }

    @Test
    public void testSetValue() {
        Column stringColumn = new Column(0, "string");
        String expectedString = "testOfSettingAString";
        stringColumn.setValue(expectedString);
        assertEquals(expectedString, stringColumn.getValue());

        Integer expectedInt = 42;
        Column integerColumn = new Column(0, "number");
        integerColumn.setValue(expectedInt);
        assertEquals(expectedInt, integerColumn.getValue());

        Column nullColumn = new Column(0, "number");
        nullColumn.setValue(null);
        assertNull(nullColumn.getValue());
    }

    @Test
    public void cloneTest() {
        Column column = new Column(0, "heroes", "duke");
        Column cloned = column.clone();
        assertNotSame(column, cloned);
        assertEquals(column, cloned);
    }

}
