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
import com.airhacks.enhydrator.out.Sink;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author airhacks.com
 */
public class VirtualSinkSource extends Sink implements Source {

    private List<List<Entry>> rows;

    public VirtualSinkSource() {
        this("*", new ArrayList<>());
    }

    public VirtualSinkSource(String name, List<List<Entry>> entries) {
        super(name);
        this.rows = entries;
    }

    public int getNumberOfRows() {
        return rows.size();
    }

    public List<Entry> getRow(int index) {
        if (index >= this.rows.size()) {
            return new ArrayList<>();
        }
        return this.rows.get(index);
    }

    /**
     * @see Source
     * @param query not applicable
     * @param params not applicable
     * @return the cached content
     */
    @Override
    public Iterable<List<Entry>> query(String query, Object... params) {
        return this.rows;
    }

    /**
     * @see Sink
     * @param entries process row
     */
    @Override
    public void processRow(List<Entry> entries) {
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

    public String mapToString(List<Entry> columns) {
        Optional<String> column = columns.stream().
                map(e -> String.valueOf(e.getValue())).
                reduce((l, r) -> l + "|" + r);
        if (column.isPresent()) {
            return column.get();
        } else {
            return "--empty--";
        }
    }

    public static class Rows {

        private List<Entry> currentRow;
        private List<List<Entry>> rows;
        private String name;

        public Rows() {
            this.rows = new ArrayList<>();
            this.currentRow = new ArrayList<>();
            this.name = "*";
        }

        public Rows addColumn(Entry entry) {
            getCurrentRow().add(entry);
            return this;
        }

        List<Entry> getCurrentRow() {
            if (this.currentRow == null) {
                this.currentRow = new ArrayList<>();
            }
            return this.currentRow;
        }

        public Rows addColumn(String name, String value) {
            int index = getCurrentRow().size() - 1;
            if (index == -1) {
                //in case row is empty
                index = 0;
            }
            return addColumn(new Entry(index, name, value));
        }

        public Rows addRow() {
            this.rows.add(getCurrentRow());
            this.currentRow = new ArrayList<>();
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
}
