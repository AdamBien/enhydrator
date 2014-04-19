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

import com.airhacks.enhydrator.db.JDBCConnection;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.JDBCSink;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
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

    public Plumber() {
        this(".", "config");
    }

    public Plumber(String baseFolder, String configurationFolder) {
        Objects.requireNonNull(baseFolder, "Base folder cannot be null");
        Objects.requireNonNull(configurationFolder, "Configuration folder cannot be null");
        this.baseFolder = baseFolder;
        this.configurationFolder = configurationFolder;
        try {
            Files.createDirectories(Paths.get(baseFolder, configurationFolder));
            this.context = JAXBContext.newInstance(JDBCSource.class,
                    JDBCPipeline.class, JDBCSink.class,
                    JDBCConnection.class, EntryTransformation.class);
            this.marshaller = context.createMarshaller();
            this.marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            this.unmarshaller = context.createUnmarshaller();
        } catch (JAXBException | IOException ex) {
            throw new IllegalStateException("Plumber construction failed ", ex);
        }
    }

    public Pipeline fromConfiguration(String pipeName) {
        Path path = getPath(pipeName);
        BufferedReader bufferedReader;
        try {
            bufferedReader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
            return (Pipeline) unmarshaller.unmarshal(bufferedReader);
        } catch (IOException | JAXBException ex) {
            throw new IllegalStateException("Cannot deserialize pipeline with name: " + pipeName, ex);
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
