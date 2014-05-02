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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 * @author airhacks.com
 */
public class Row {

    private final int index;
    private final Map<String, Object> row;
    private final Map<String, String> destinations;
    private final String DEFAULT_DESTINATION = "*";
    private String name;
    private static final String DESTINATION = "destination";

    public Row(int index) {
        this.index = index;
        this.row = new ConcurrentHashMap<>();
        this.destinations = new ConcurrentHashMap<>();
    }

    public int getIndex() {
        return index;
    }

    public Object getColumn(String column) {
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
        this.row.put(name, value);
        this.destinations.put(name, DEFAULT_DESTINATION);
        return this;
    }

    public Row addColumn(String name, String destination, Object value) {
        this.row.put(name, value);
        this.destinations.put(name, destination);
        return this;
    }

    public void transformColumn(String name, Function<Object, Object> transformer) {
        Object input = getColumn(name);
        if (input == null) {
            return;
        }
        Object output = transformer.apply(input);
        this.row.put(name, output);
    }

    public int getNumberOfColumns() {
        return this.row.size();
    }

    public Map<String, Object> getColumns() {
        return this.row;
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
        return this.destinations.getOrDefault(columnName, DEFAULT_DESTINATION);
    }

    public boolean isNumber(String column) {
        return (this.row.get(column) instanceof Number);
    }

    public boolean isString(String column) {
        return (this.row.get(column) instanceof String);
    }

    public Row changeDestination(String column, String newDestination) {
        this.destinations.put(column, newDestination);
        return this;
    }

    public boolean isEmpty() {
        return this.row.isEmpty();
    }

    public Map<String, Row> getColumnsGroupedByDestination() {
        Map<String, List<Map.Entry<String, String>>> rawGrouping = this.destinations.
                entrySet().
                stream().
                collect(Collectors.groupingBy(e -> e.getValue()));
        Map<String, Row> groupedRows = new HashMap<>();
        rawGrouping.entrySet().forEach(e -> groupedRows.put(e.getKey(), convert(e.getValue())));
        return groupedRows;
    }

    Row convert(List<Map.Entry<String, String>> content) {
        Row newRow = new Row(index);
        content.forEach(e -> newRow.addColumn(e.getKey(), this.row.get(e.getKey())));
        return newRow;
    }
}
