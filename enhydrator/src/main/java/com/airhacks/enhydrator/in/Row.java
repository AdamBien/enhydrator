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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 *
 * @author airhacks.com
 */
public class Row {

    private final int index;
    private final Map<String, Object> row;
    private final Map<String, Map<String, String>> meta;
    private final String DEFAULT_DESTINATION = "*";
    private String name;
    private static final String DESTINATION = "destination";

    public Row(int index) {
        this.index = index;
        this.row = new ConcurrentHashMap<>();
        this.meta = new ConcurrentHashMap<>();
    }

    public int getIndex() {
        return index;
    }

    public Object getColumn(String column) {
        return this.row.get(column);
    }

    public Row addColumn(String name, Object value) {
        this.row.put(name, value);
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
        return this.meta.getOrDefault(columnName, new HashMap<>()).
                getOrDefault(DESTINATION, DEFAULT_DESTINATION);
    }

    String getMeta(String columnName, String key) {
        return this.meta.getOrDefault(columnName, new HashMap<>()).get(key);
    }

    public boolean isNumber(String column) {
        return (this.row.get(column) instanceof Number);
    }

    public boolean isString(String column) {
        return (this.row.get(column) instanceof String);
    }

    public Row changeDestination(String column, String newDestination) {
        getMetaForColumn(name).put(DESTINATION, newDestination);
        return this;
    }

    public Map<String, String> getMetaForColumn(String name) {
        Map<String, String> columnMeta = this.meta.get(name);
        if (columnMeta == null) {
            columnMeta = new HashMap<>();
            this.meta.put(name, columnMeta);
        }
        return columnMeta;
    }

    public boolean isEmpty() {
        return this.row.isEmpty();
    }
}
