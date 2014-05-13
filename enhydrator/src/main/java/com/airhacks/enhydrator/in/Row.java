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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * @author airhacks.com
 */
public class Row {

    private final Map<String, Column> rowByName;
    private final Map<Integer, Column> rowByIndex;

    private List<Row> children;

    public Row() {
        this.rowByName = new ConcurrentHashMap<>();
        this.rowByIndex = new ConcurrentHashMap<>();
        this.children = new CopyOnWriteArrayList<>();
    }

    public Object getColumnValue(String columnName) {
        final Column column = this.rowByName.get(columnName);
        if (column == null || column.isNullValue()) {
            return null;
        }
        return column.getValue();
    }

    public Column getColumnByName(String column) {
        return this.rowByName.get(column);
    }

    public Column getColumnByIndex(int index) {
        return this.rowByIndex.get(index);
    }

    /**
     * Adds or overrides a column with a default destination
     *
     * @param index the origin index
     * @param name a unique name of the column.
     * @param value
     * @return reference for method chaining
     */
    public Row addColumn(int index, String name, Object value) {
        Objects.requireNonNull(name, "Name of the column cannot be null");
        Objects.requireNonNull(value, "Value of " + name + " cannot be null");
        final Column column = new Column(index, name, value);
        this.rowByName.put(name, column);
        this.rowByIndex.put(index, column);
        return this;
    }

    public Row addColumn(Column column) {
        Objects.requireNonNull(column, "Column cannot be null");
        this.rowByName.put(column.getName(), column);
        this.rowByIndex.put(column.getIndex(), column);
        return this;
    }

    public Row addNullColumn(int index, String name) {
        final Column column = new Column(index, name);
        this.rowByName.put(name, column);
        this.rowByIndex.put(index, column);
        return this;
    }

    public void transformColumn(String name, Function<Object, Object> transformer) {
        Column input = getColumnByName(name);
        if (input == null || input.isNullValue()) {
            return;
        }
        Object output = transformer.apply(input.getValue());
        input.setValue(output);
    }

    public int getNumberOfColumns() {
        return this.rowByName.size();
    }

    public Map<String, Object> getColumnValues() {
        return this.rowByName.entrySet().
                stream().filter(e -> e.getValue().getValue() != null).
                collect(Collectors.toMap(k -> k.getKey(), v -> value(v)));
    }

    Object value(Entry<String, Column> entry) {
        Objects.requireNonNull(entry, "Entry cannot be null");
        String columnName = entry.getKey();
        Column column = entry.getValue();
        Objects.requireNonNull(columnName, "Column name cannot be null");
        Objects.requireNonNull(column, "Column with name " + columnName + " is null");
        Object value = column.getValue();
        Objects.requireNonNull(value, "Column with name " + columnName + " has null value");
        return value;
    }

    public Set<String> getColumnNames() {
        return this.rowByName.keySet();
    }

    public Map<String, String> getColumnsAsString() {
        Map<String, String> retVal = new HashMap<>();
        this.rowByName.keySet().forEach(e -> retVal.put(e, String.valueOf(this.rowByName.get(e))));
        return retVal;
    }

    public Row removeColumn(String name) {
        this.rowByName.remove(name);
        return this;
    }

    public String getDestination(String columnName) {
        return this.rowByName.get(columnName).getTargetSink();
    }

    public boolean isNumber(String column) {
        return (this.rowByName.get(column).isNumber());
    }

    public boolean isString(String column) {
        return (this.rowByName.get(column).isString());
    }

    public Row changeDestination(String column, String newDestination) {
        getColumnByName(column).setTargetSink(newDestination);
        return this;
    }

    public boolean isEmpty() {
        return this.rowByName.isEmpty();
    }

    public Map<String, Row> getColumnsGroupedByDestination() {
        Map<String, List<Map.Entry<String, Column>>> grouped = this.rowByName.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue().getTargetSink()));
        return grouped.entrySet().stream().
                collect(Collectors.toMap(k -> k.getKey(), v -> convert(v.getValue())));
    }

    public Row convert(List<Map.Entry<String, Column>> content) {
        Row copy = new Row();
        copy.children = this.children;
        content.forEach(c -> copy.addColumn(c.getValue()));
        return copy;
    }

    public boolean isColumnEmpty(String name) {
        return !this.rowByName.containsKey(name);
    }

    public Row add(Row input) {
        this.children.add(input);
        return this;
    }

    public List<Row> getChildren() {
        return this.children;
    }

    public boolean hasChildren() {
        return !this.children.isEmpty();
    }

}
