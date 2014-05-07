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

    private final Map<String, Column> row;
    private List<Row> children;

    public Row() {
        this.row = new ConcurrentHashMap<>();
        this.children = new CopyOnWriteArrayList<>();
    }

    public Object getColumnValue(String columnName) {
        final Column column = this.row.get(columnName);
        if (column == null) {
            return null;
        }
        return column.getValue();
    }

    public Column getColumn(String column) {
        return this.row.get(column);
    }

    /**
     * Adds or overrides a column with a default destination
     *
     * @param name a unique name of the column.
     * @param value
     * @return reference for method chaining
     */
    public Row addColumn(String name, Object value) {
        Objects.requireNonNull(name, "Name of the column cannot be null");
        Objects.requireNonNull(value, "Value of " + name + " cannot be null");
        this.row.put(name, new Column(name, value));
        return this;
    }

    public Row addNullColumn(String name) {
        this.addColumn(name, new Column(name));
        return this;
    }

    public Row createColumn(String name, String destination, Object value) {
        this.row.put(name, new Column(name, destination, value));
        return this;
    }

    public Row addColumn(String name, Column column) {
        this.row.put(name, column);
        return this;
    }

    public void transformColumn(String name, Function<Object, Object> transformer) {
        Column input = getColumn(name);
        if (input == null || input.isNullValue()) {
            return;
        }
        Object output = transformer.apply(input.getValue());
        input.setValue(output);
    }

    public int getNumberOfColumns() {
        return this.row.size();
    }

    public Map<String, Object> getColumnValues() {
        return this.row.entrySet().
                stream().
                collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue().
                                getValue()));
    }

    public Set<String> getColumnNames() {
        return this.row.keySet();
    }

    public Map<String, String> getColumnsAsString() {
        Map<String, String> retVal = new HashMap<>();
        this.row.keySet().forEach(e -> retVal.put(e, String.valueOf(this.row.get(e))));
        return retVal;
    }

    public Row removeColumn(String name) {
        this.row.remove(name);
        return this;
    }

    public String getDestination(String columnName) {
        return this.row.get(columnName).getDestination();
    }

    public boolean isNumber(String column) {
        return (this.row.get(column).isNumber());
    }

    public boolean isString(String column) {
        return (this.row.get(column).isString());
    }

    public Row changeDestination(String column, String newDestination) {
        getColumn(column).setDestination(newDestination);
        return this;
    }

    public boolean isEmpty() {
        return this.row.isEmpty();
    }

    public Map<String, Row> getColumnsGroupedByDestination() {
        Map<String, List<Map.Entry<String, Column>>> grouped = this.row.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue().getDestination()));
        return grouped.entrySet().stream().
                collect(Collectors.toMap(k -> k.getKey(), v -> convert(v.getValue())));
    }

    public Row convert(List<Map.Entry<String, Column>> content) {
        Row copy = new Row();
        copy.children = this.children;
        content.forEach(c -> copy.addColumn(c.getKey(), c.getValue()));
        return copy;
    }

    public boolean isColumnEmpty(String name) {
        return !this.row.containsKey(name);
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
