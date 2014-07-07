/*
 * Copyright 2014 Adam Bien.
 *
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
 */
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
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author airhacks.com
 */
public class CSVStreamSource implements Source {

    private final String charsetName;
    private final String delimiter;
    private final boolean containsHeaders;

    private Charset charset;

    static final String REGEX_SPLIT_EXPRESSION = "(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    private Stream<String> lines;

    private List<String> columnNames;
    private List<Row> iterable;

    private InputStream stream;

    public CSVStreamSource(InputStream stream, String delimiter, String charset, boolean containsHeaders) {
        this.stream = stream;
        this.delimiter = delimiter;
        this.containsHeaders = containsHeaders;
        this.charsetName = charset;
        init();
    }

    void init() throws IllegalStateException, IllegalArgumentException {
        this.charset = Charset.forName(charsetName);
        this.columnNames = new ArrayList<>();
        this.lines = new BufferedReader(new InputStreamReader(stream, this.charset)).lines();
    }

    void extractHeaders(int index, String headerLine) {
        Row headers = new Row();
        cellToRow(headers, headerLine, index);
        for (int i = 0; i < headers.getNumberOfColumns(); i++) {
            Column columnByIndex = headers.getColumnByIndex(i);
            this.columnNames.add(String.valueOf(columnByIndex.getValue()));
        }
    }

    /**
     *
     * @param query not supported yet
     * @param params not supported yet
     * @return
     */
    @Override
    public Iterable<Row> query(String query, Object... params) {
        if (this.iterable == null) {
            this.iterable = this.lines.map(s -> parse(s, this.delimiter)).collect(Collectors.toList());
        }
        return this.iterable;
    }

    Row parse(String line, String delimiter) {
        String[] splitted = split(line, delimiter + REGEX_SPLIT_EXPRESSION);
        if (splitted == null || splitted.length == 0) {
            return null;
        }
        Row row = new Row();
        for (int i = 0; i < splitted.length; i++) {
            String slot = splitted[i];
            if (this.containsHeaders && i == 0) {
                this.extractHeaders(i, slot);
                continue;
            }
            cellToRow(row, slot, i);
        }
        return row;
    }

    void cellToRow(Row row, String slot, int index) {
        String columnName = getColumnName(index);
        if (slot.isEmpty()) {
            row.addNullColumn(index, columnName);
        } else {
            row.addColumn(index, columnName, slot);
        }
    }

    static String[] split(String line, String delimiter) {
        return line.split(delimiter, -1);
    }

    String getColumnName(int slot) {
        if (slot < this.columnNames.size()) {
            return this.columnNames.get(slot);
        } else {
            String name = String.valueOf(slot);
            if (slot >= this.columnNames.size()) {
                this.columnNames.add(name);
            } else {
                this.columnNames.set(slot, name);
            }
            return name;
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + Objects.hashCode(this.charsetName);
        hash = 23 * hash + Objects.hashCode(this.delimiter);
        hash = 23 * hash + (this.containsHeaders ? 1 : 0);
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
        final CSVStreamSource other = (CSVStreamSource) obj;
        if (!Objects.equals(this.charsetName, other.charsetName)) {
            return false;
        }
        if (!Objects.equals(this.delimiter, other.delimiter)) {
            return false;
        }
        if (this.containsHeaders != other.containsHeaders) {
            return false;
        }
        return true;
    }

}
