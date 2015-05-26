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
import com.airhacks.enhydrator.transform.Memory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 *
 * @author airhacks.com
 */
public class Row {

    private final Map<String, Column> columnByName;
    private final Map<Integer, Column> columnByIndex;

    private List<Row> children;
    private Memory memory;

    public Row() {
        this.columnByName = new ConcurrentHashMap<>();
        this.columnByIndex = new ConcurrentHashMap<>();
        this.children = new CopyOnWriteArrayList<>();
    }

    public void useMemory(Memory globalMemory) {
        this.memory = globalMemory;
    }

    public Object getColumnValue(String columnName) {
        final Column column = this.columnByName.get(columnName);
        if (column == null || column.isNullValue()) {
            return null;
        }
        return column.getValue();
    }

    public Column getColumnByName(String column) {
        return this.columnByName.get(column);
    }

    public void findColumnsAndApply(Predicate<Column> predicate, Consumer<Column> modifier) {
        this.columnByName.values().stream().filter(predicate).forEach(modifier);
    }

    public void findColumnsAndChangeName(Predicate<Column> predicate, Function<Column, String> renamingFunction) {
        List<Column> collect = this.columnByName.values().stream().
                filter(predicate).collect(Collectors.toList());
        collect.stream().forEach(c -> this.changeColumnName(c.getName(), renamingFunction.apply(c)));

    }

    public Column getColumnByIndex(int index) {
        return this.columnByIndex.get(index);
    }

    public Collection<Column> getColumns() {
        return this.columnByName.values();
    }

    public List<Column> getColumnsSortedByColumnIndex() {
        return this.columnByName.values().stream().
                sorted((col1, col2) -> Integer.compare(col1.getIndex(), col2.getIndex())).
                collect(Collectors.toList());
    }

    public void changeColumnName(String oldName, String newName) {
        Column column = this.columnByName.remove(oldName);
        if (column == null) {
            return;
        }
        column.setName(newName);
        this.columnByName.put(newName, column);
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
        return this.addColumn(column);
    }

    public Row addColumn(Column column) {
        Objects.requireNonNull(column, "Column cannot be null");
        this.columnByName.put(column.getName(), column);
        this.columnByIndex.put(column.getIndex(), column);
        return this;
    }

    public Row addNullColumn(int index, String name) {
        final Column column = new Column(index, name);
        this.columnByName.put(name, column);
        this.columnByIndex.put(index, column);
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
        return this.columnByName.size();
    }

    public Map<String, Optional<Object>> getColumnValues() {
        return this.columnByName.entrySet().stream().
                collect(Collectors.toMap(k -> k.getKey(), v -> value(v)));
    }

    Optional<Object> value(Entry<String, Column> entry) {
        Objects.requireNonNull(entry, "Entry cannot be null");
        String columnName = entry.getKey();
        Column column = entry.getValue();
        Objects.requireNonNull(columnName, "Column name cannot be null");
        Objects.requireNonNull(column, "Column with name " + columnName + " is null");
        Optional<Object> valueAsOptional = column.getValueAsOptional();
        return valueAsOptional;
    }

    public Set<String> getColumnNames() {
        return this.columnByName.keySet();
    }

    public Map<String, String> getColumnsAsString() {
        Map<String, String> retVal = new HashMap<>();
        this.columnByName.keySet().forEach(e -> retVal.put(e, String.valueOf(this.columnByName.get(e))));
        return retVal;
    }

    public Row removeColumn(String name) {
        this.columnByName.remove(name);
        return this;
    }

    public String getDestination(String columnName) {
        return this.columnByName.get(columnName).getTargetSink();
    }

    public boolean isNumber(String column) {
        return (this.columnByName.get(column).isNumber());
    }

    public boolean isString(String column) {
        return (this.columnByName.get(column).isString());
    }

    public Row changeDestination(String column, String newDestination) {
        getColumnByName(column).setTargetSink(newDestination);
        return this;
    }

    public boolean isEmpty() {
        return this.columnByName.isEmpty();
    }

    public List<String> getSortedColumnNames() {
        List<String> sortedColumnNames = new ArrayList<>();
        for (int i = 0; i < this.columnByIndex.size(); i++) {
            Column column = this.columnByIndex.get(i);
            if (column == null) {
                sortedColumnNames.add("-");
            } else {
                sortedColumnNames.add(column.getName());
            }
        }
        return sortedColumnNames;
    }

    public Map<String, Row> getColumnsGroupedByDestination() {
        Map<String, List<Map.Entry<String, Column>>> grouped = this.columnByName.entrySet().stream().collect(Collectors.groupingBy(e -> e.getValue().getTargetSink()));
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
        return !this.columnByName.containsKey(name);
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

    public Memory getMemory() {
        return memory;
    }

    public void successfullyProcessed() {
        this.memory.processed();
    }

    public void errorOccured() {
        this.memory.errorOccured();
    }

    public void errorOccured(Throwable ex) {
        this.memory.addProcessingError(this, ex);
    }

    public void reindexColumns() {
        this.columnByIndex.clear();
        this.columnByName.values().forEach(col -> this.columnByIndex.put(col.getIndex(), col));
    }

    @Override
    public String toString() {
        return "Row{" + "columnByName=" + columnByName + ", columnByIndex=" + columnByIndex + ", children=" + children + '}';
    }
}
