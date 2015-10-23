package com.airhacks.enhydrator.flexpipe;

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
import com.airhacks.enhydrator.db.UnmanagedConnectionProvider;
import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.CSVStreamSource;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.in.ScriptableSource;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.CSVFileSink;
import com.airhacks.enhydrator.out.JDBCSink;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.out.ScriptableSink;
import com.airhacks.enhydrator.transform.ColumnCopier;
import com.airhacks.enhydrator.transform.Datatype;
import com.airhacks.enhydrator.transform.DatatypeIndexMapper;
import com.airhacks.enhydrator.transform.DatatypeNameMapper;
import com.airhacks.enhydrator.transform.DestinationMapper;
import com.airhacks.enhydrator.transform.NameMapper;
import com.airhacks.enhydrator.transform.NashornRowTransformer;
import com.airhacks.enhydrator.transform.SkipFirstRow;
import com.airhacks.enhydrator.transform.TargetMapping;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 *
 * @author airhacks.com
 */
public class Plumber {

    private JAXBContext context;
    private Marshaller marshaller;
    private Unmarshaller unmarshaller;

    private String baseFolder;
    private String configurationFolder;

    public static Plumber createWithDefaultPath() {
        return new Plumber(".", "config");
    }

    public static Plumber createWith(String baseFolder, String configurationFolder) {
        return new Plumber(baseFolder, configurationFolder);
    }

    public static Plumber createWithoutPath() {
        return new Plumber();
    }

    private Plumber() {
        this.init();
    }

    private Plumber(String baseFolder, String configurationFolder) {
        Objects.requireNonNull(baseFolder, "Base folder cannot be null");
        Objects.requireNonNull(configurationFolder, "Configuration folder cannot be null");
        this.baseFolder = baseFolder;
        this.configurationFolder = configurationFolder;
        try {
            Files.createDirectories(Paths.get(baseFolder, configurationFolder));
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot create directories for plumber ", ex);
        }
        this.init();
    }

    final void init() {
        try {
            this.context = JAXBContext.newInstance(JDBCSource.class,
                    CSVFileSource.class, CSVStreamSource.class, VirtualSinkSource.class,
                    Pipeline.class, JDBCSink.class, LogSink.class,
                    UnmanagedConnectionProvider.class, ColumnTransformation.class,
                    NashornRowTransformer.class, DestinationMapper.class,
                    TargetMapping.class, DatatypeIndexMapper.class, DatatypeNameMapper.class,
                    Datatype.class, SkipFirstRow.class, ScriptableSource.class,
                    CSVFileSink.class, NameMapper.class, ColumnCopier.class,
                    ScriptableSink.class);
            this.marshaller = context.createMarshaller();
            this.marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            this.unmarshaller = context.createUnmarshaller();
        } catch (JAXBException ex) {
            throw new IllegalStateException("Plumber construction failed ", ex);
        }
    }

    public Pipeline fromConfiguration(String pipeName) {
        Path path = getPath(pipeName);
        try {
            BufferedReader bufferedReader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
            return fromInputStream(bufferedReader);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot deserialize pipeline with name: " + pipeName, ex);
        }
    }

    public Pipeline fromInputStream(Reader reader) {
        try {
            return (Pipeline) unmarshaller.unmarshal(reader);
        } catch (JAXBException ex) {
            throw new IllegalStateException("Cannot deserialize pipeline from input stream ", ex);
        }
    }

    Path getPath(String pipeName) {
        Objects.requireNonNull(pipeName, "Path has to be set");
        return Paths.get(baseFolder, configurationFolder, pipeName + ".xml");
    }

    public void intoConfiguration(Pipeline pipeline) {
        Objects.requireNonNull(pipeline, "Cannot serialize a null pipeline");
        String name = pipeline.getName();
        Path path = getPath(name);
        BufferedWriter writer;
        try {
            writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
            marshaller.marshal(pipeline, writer);
        } catch (IOException | JAXBException ex) {
            throw new IllegalStateException("Cannot create configuration for pipeline ", ex);
        }
    }
}
