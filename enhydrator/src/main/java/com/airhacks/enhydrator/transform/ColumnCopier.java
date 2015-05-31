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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "column-copier")
@XmlAccessorType(XmlAccessType.FIELD)
public class ColumnCopier extends RowTransformation {

    Map<String, List<String>> columnMappings;

    public ColumnCopier() {
        this.columnMappings = new HashMap<>();
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

    void copyColumn(Row row, Column column, List<String> names) {
        if (column == null) {
            return;
        }
        names.forEach(n -> row.addColumn(copy(n, column)));
    }

    Column copy(String name, Column column) {
        Column clone = column.clone();
        clone.setName(name);
        return clone;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 89 * hash + Objects.hashCode(this.columnMappings);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
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
