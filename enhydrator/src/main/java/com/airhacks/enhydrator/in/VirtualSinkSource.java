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
import com.airhacks.enhydrator.out.NamedSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "virtual-sink-source")
public class VirtualSinkSource extends NamedSink implements Source {

    @XmlTransient
    private List<Row> rows;

    public VirtualSinkSource() {
        this("*", new ArrayList<>());
    }

    public VirtualSinkSource(String name, List<Row> entries) {
        super(name);
        this.rows = entries;
    }

    public int getNumberOfRows() {
        return rows.size();
    }

    public List<Row> getRows() {
        return this.rows;
    }

    public Row getRow(int index) {
        if (index >= this.rows.size()) {
            return null;
        }
        return this.rows.get(index);
    }

    public VirtualSinkSource addRow(Row row) {
        this.rows.add(row);
        return this;
    }

    /**
     * @see Source
     * @param query not applicable
     * @param params not applicable
     * @return the cached content
     */
    @Override
    public Iterable<Row> query(String query, Object... params) {
        return this.rows;
    }

    /**
     * @see NamedSink
     * @param entries process row
     */
    @Override
    public void processRow(Row entries) {
        this.rows.add(entries);
    }

    @Override
    public String toString() {
        Optional<String> representation = this.rows.stream().map(l -> mapToString(l)).reduce((l, r) -> l + "\n" + r);
        if (!representation.isPresent()) {
            return "--empty--";
        } else {
            return representation.get();
        }
    }

    public String mapToString(Row columns) {
        Optional<String> column = columns.getColumnValues().values().stream().
                map(e -> String.valueOf(e.toString())).
                reduce((l, r) -> l + "|" + r);
        if (column.isPresent()) {
            return column.get();
        } else {
            return "--empty--";
        }
    }

    public static class Rows {

        private Row currentRow;
        private List<Row> rows;
        private String name;

        public Rows() {
            this.rows = new ArrayList<>();
            this.currentRow = new Row();
            this.name = "*";
        }

        Row getCurrentRow() {
            return this.currentRow;
        }

        public Rows addColumn(int index, String name, String value) {
            this.currentRow.addColumn(index, name, value);
            return this;
        }

        public Rows addColumn(String name, String value) {
            return addColumn(-1, name, value);
        }

        public Rows addRow() {
            this.rows.add(getCurrentRow());
            this.currentRow = new Row();
            return this;
        }

        public Rows sinkName(String name) {
            this.name = name;
            return this;
        }

        public VirtualSinkSource build() {
            VirtualSinkSource vss = new VirtualSinkSource();
            vss.name = this.name;
            if (!this.rows.isEmpty()) {
                vss.rows = this.rows;
            }
            return vss;
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
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
        return true;
    }

}
