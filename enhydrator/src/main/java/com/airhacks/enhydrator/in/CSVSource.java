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
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "csv-source")
public class CSVSource implements Source {

    private String fileName;
    private String charsetName;
    private String delimiter;
    private boolean fileContainsHeaders;

    @XmlTransient
    private Charset charset;

    static final String REGEX_SPLIT_EXPRESSION = "(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    @XmlTransient
    private Path file;
    @XmlTransient
    private Stream<String> lines;

    @XmlTransient
    private int counter;

    @XmlTransient
    private List<String> columnNames;
    @XmlTransient
    private List<Row> iterable;

    public CSVSource(String file, String delimiter, String charset, boolean fileContainsHeaders) {
        this.fileName = file;
        this.delimiter = delimiter;
        this.fileContainsHeaders = fileContainsHeaders;
        this.charsetName = charset;
        init();
    }

    public CSVSource() {
    }

    void afterUnmarshal(Unmarshaller umarshaller, Object parent) {
        this.init();
    }

    void init() throws IllegalStateException, IllegalArgumentException {
        this.charset = Charset.forName(charsetName);
        this.file = Paths.get(this.fileName);
        if (!Files.exists(this.file)) {
            throw new IllegalArgumentException(this.fileName + " does not exist !");
        }
        this.columnNames = new ArrayList<>();
        try {
            this.lines = Files.lines(this.file, this.charset);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot parse lines");
        }
        if (this.fileContainsHeaders) {
            this.columnNames = this.extractHeaders();
        }
    }

    final List<String> extractHeaders() {
        String headerLine = null;
        try {
            headerLine = Files.lines(this.file, this.charset).findFirst().get();
        } catch (IOException ex) {
            Logger.getLogger(CSVSource.class.getName()).log(Level.SEVERE, null, ex);
        }
        Row headers = parse(headerLine, this.delimiter);

        return headers.getColumnValues().values().stream().
                map(e -> ((String) e)).
                collect(Collectors.toList());
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
            String value = splitted[i];
            String columnName = getColumnName(i);
            if (value.isEmpty()) {
                row.addNullColumn(i, columnName);
            } else {
                row.addColumn(i, columnName, value);
            }
        }
        return row;
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
        hash = 23 * hash + Objects.hashCode(this.fileName);
        hash = 23 * hash + Objects.hashCode(this.charsetName);
        hash = 23 * hash + Objects.hashCode(this.delimiter);
        hash = 23 * hash + (this.fileContainsHeaders ? 1 : 0);
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
        final CSVSource other = (CSVSource) obj;
        if (!Objects.equals(this.fileName, other.fileName)) {
            return false;
        }
        if (!Objects.equals(this.charsetName, other.charsetName)) {
            return false;
        }
        if (!Objects.equals(this.delimiter, other.delimiter)) {
            return false;
        }
        if (this.fileContainsHeaders != other.fileContainsHeaders) {
            return false;
        }
        return true;
    }

}
