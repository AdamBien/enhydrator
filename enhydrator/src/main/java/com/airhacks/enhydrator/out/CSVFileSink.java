package com.airhacks.enhydrator.out;

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
import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
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
@XmlRootElement(name = "csv-file-sink")
public class CSVFileSink extends Sink {

    private String fileName;
    private String delimiter;
    private boolean append;
    private boolean useNamesAsHeaders;
    private String charsetName = StandardCharsets.UTF_8.displayName();
    private Charset charset;

    @XmlTransient
    private boolean namesAlreadyWritten = false;
    @XmlTransient
    PrintWriter bos;

    public CSVFileSink(String sinkName, String fileName, String delimiter, boolean useNamesAsHeaders, boolean append, String charsetName) {
        super(sinkName);
        this.fileName = fileName;
        this.delimiter = delimiter;
        this.append = append;
        this.useNamesAsHeaders = useNamesAsHeaders;
        this.charsetName = charsetName;
    }

    public CSVFileSink(String sinkName, String fileName, String delimiter, boolean useNamesAsHeaders, boolean append) {
        super(sinkName);
        this.fileName = fileName;
        this.delimiter = delimiter;
        this.append = append;
        this.useNamesAsHeaders = useNamesAsHeaders;
    }

    CSVFileSink() {
        //required for JAXB
    }

    void afterUnmarshal(Unmarshaller umarshaller, Object parent) {
        this.init();
    }

    @Override
    public void init() {
        this.charset = Charset.forName(charsetName);
        try {
            this.bos = new PrintWriter(new OutputStreamWriter(new FileOutputStream(fileName, append), charset));
        } catch (IOException ex) {
            throw new IllegalStateException("File " + this.fileName + " not found", ex);
        }
    }

    @Override
    public void processRow(Row entries) {
        Collection<Column> columns = entries.getColumns();
        if (this.useNamesAsHeaders && !this.namesAlreadyWritten) {
            String header = columns.stream().map(c -> c.getName()).
                    reduce((t, u) -> t + delimiter + u).get();
            write(header);
            this.namesAlreadyWritten = true;
        }
        String line = columns.stream().map(c -> c.getValue().toString()).
                reduce((t, u) -> t + delimiter + u).get();
        write(line);
    }

    void write(String line) {
        this.bos.println(line);
    }

    @Override
    public void close() {
        this.bos.flush();
        this.bos.close();
    }

}
