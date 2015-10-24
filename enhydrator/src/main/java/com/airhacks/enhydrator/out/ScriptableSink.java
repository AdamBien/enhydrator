package com.airhacks.enhydrator.out;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 - 2015 Adam Bien
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
import com.airhacks.enhydrator.in.Row;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import static java.util.Objects.requireNonNull;

/**
 *
 * @author airhacks.com
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "scriptable-sink")
public class ScriptableSink extends NamedSink {

    private static final String DEFAULT_NAME = "script";

    @XmlTransient
    private ScriptEngine engine;
    @XmlTransient
    private Invocable invocable;

    @XmlElement(name = "script-file")
    private String scriptFile;

    @XmlTransient
    private String scriptContent;

    @XmlTransient
    private Sink sink;

    public ScriptableSink(String scriptFile) {
        super(DEFAULT_NAME);
        this.scriptFile = scriptFile;
    }

    public ScriptableSink(String name, String scriptFile) {
        super(name);
        this.scriptFile = scriptFile;
    }

    public ScriptableSink() {
    }

    public String load(String file) throws IOException {
        return new String(Files.readAllBytes(Paths.get(file)), "UTF-8");
    }

    @Override
    public void init() {
        requireNonNull(this.scriptFile, "Cannot initialize ScriptableSink without script.");
        try {
            this.scriptContent = load(this.scriptFile);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot load script from: " + this.scriptFile, ex);
        }
        ScriptEngineManager sem = new ScriptEngineManager();
        this.engine = sem.getEngineByName("javascript");
        try {
            this.engine.eval(this.scriptContent);
        } catch (ScriptException ex) {
            throw new IllegalStateException("Parsing script failed. Problem in line: "
                    + ex.getLineNumber() + " and column: " + ex.getColumnNumber(), ex);
        }

        this.invocable = (Invocable) engine;

        this.sink = this.invocable.getInterface(Sink.class);
        requireNonNull(this.sink, "Sink instantiation failed - script: " + this.scriptContent + " is incompatible");
        this.sink.init();
    }

    @Override
    public void processRow(Row entries) {
        this.sink.processRow(entries);
    }

    @Override
    public void close() {
        this.sink.close();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 47 * hash + Objects.hashCode(this.scriptFile);
        hash = 47 * hash + Objects.hashCode(this.scriptContent);
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
        final ScriptableSink other = (ScriptableSink) obj;
        if (!Objects.equals(this.scriptFile, other.scriptFile)) {
            return false;
        }
        if (!Objects.equals(this.scriptContent, other.scriptContent)) {
            return false;
        }
        return true;
    }

}
