package com.airhacks.enhydrator.transform;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2015 Adam Bien
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
import com.airhacks.enhydrator.flexpipe.RowTransformation;
import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "column-copier")
@XmlAccessorType(XmlAccessType.FIELD)
public class ColumnCopier extends RowTransformation {

    Map<String, NameList> columnMappings;

    public ColumnCopier(Map<String, NameList> columnMappings) {
        this.columnMappings = columnMappings;
    }

    public ColumnCopier() {
        this.columnMappings = new HashMap<>();
    }

    public void addMapping(String columnName, List<String> targetNames) {
        this.columnMappings.put(columnName, new NameList(targetNames));
    }

    public void addMapping(String columnName, String... targetNames) {
        this.addMapping(columnName, Arrays.asList(targetNames));
    }

    @Override
    public Row execute(Row row) {
        if (row == null) {
            return null;
        }
        List<Column> columnsToCopy = columnMappings.keySet().stream().
                filter(f -> row.getColumnByName(f) != null).
                map(m -> row.getColumnByName(m)).
                collect(Collectors.toList());
        columnsToCopy.forEach(c -> copyColumn(row, c, columnMappings.get(c.getName())));

        return row;
    }

    void copyColumn(Row row, Column column, NameList names) {
        if (column == null) {
            return;
        }
        names.getColumnList().forEach(n -> row.addColumn(copy(n, column)));
    }

    Column copy(String name, Column column) {
        Column clone = column.clone();
        clone.setName(name);
        return clone;
    }

    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class NameList {

        @XmlElement(name = "column")
        List<String> columnList;

        public NameList(List<String> columnList) {
            this.columnList = columnList;
        }

        public NameList() {
            this.columnList = new ArrayList<>();
        }

        public List<String> getColumnList() {
            return columnList;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 37 * hash + Objects.hashCode(this.columnList);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final NameList other = (NameList) obj;
            if (!Objects.equals(this.columnList, other.columnList)) {
                return false;
            }
            return true;
        }

    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 61 * hash + Objects.hashCode(this.columnMappings);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ColumnCopier other = (ColumnCopier) obj;
        if (!Objects.equals(this.columnMappings, other.columnMappings)) {
            return false;
        }
        return true;
    }

}
