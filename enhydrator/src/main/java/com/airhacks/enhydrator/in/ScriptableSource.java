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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
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
@XmlRootElement(name = "scriptable-source")
public class ScriptableSource implements Source {

    private String charsetName;

    @XmlTransient
    private Charset charset;

    @XmlTransient
    private List<Row> iterable;

    @XmlTransient
    private Path input;

    @XmlTransient
    private ScriptEngine nashorn;
    @XmlTransient
    private Reader script;
    @XmlTransient
    private List<Row> rows;
    private String inputFile;
    private String scriptFile;

    ScriptableSource() {
    }

    public ScriptableSource(String inputFile, String scriptFile, String charset) {
        this.inputFile = inputFile;
        this.scriptFile = scriptFile;
        this.charsetName = charset;
        this.preInitialize();
        this.init();
    }

    public ScriptableSource(Path path, Reader script, String charset) {
        this.input = path;
        this.charsetName = charset;
        this.script = script;
        init();
    }

    void preInitialize() {
        this.input = Paths.get(this.inputFile);
        try {
            this.script = new FileReader(this.scriptFile);
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException("Cannot read script file " + this.scriptFile, ex);
        }
    }

    void afterUnmarshal(Unmarshaller umarshaller, Object parent) {
        this.preInitialize();
        this.init();
    }

    static FileReader getScriptContents(String location) {
        try {
            return new FileReader(location);
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException("Cannot find file: " + location, ex);
        }
    }

    public void init() throws IllegalStateException, IllegalArgumentException {
        this.rows = new ArrayList<>();
        this.charset = Charset.forName(charsetName);
        ScriptEngineManager manager = new ScriptEngineManager();
        this.nashorn = manager.getEngineByName("javascript");
    }

    String pullContent() throws IOException {
        return new String(Files.readAllBytes(this.input), this.charset);

    }

    List<Row> load() throws ScriptException, IOException {
        this.nashorn.put("INPUT", pullContent());
        this.nashorn.put("ROWS", this.rows);
        return (List<Row>) this.nashorn.eval(script);
    }

    public void setInput(Path input) {
        this.input = input;
    }

    public void setScriptFile(String scriptFile) {
        this.scriptFile = scriptFile;
        try {
            this.script = new FileReader(this.scriptFile);
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException("Cannot read script file " + this.scriptFile, ex);
        }
    }

    public String getScriptFile() {
        return scriptFile;
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
            try {
                this.iterable = this.load();
            } catch (ScriptException | IOException ex) {
                Logger.getLogger(ScriptableSource.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return this.iterable;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + Objects.hashCode(this.charsetName);
        hash = 67 * hash + Objects.hashCode(this.inputFile);
        hash = 67 * hash + Objects.hashCode(this.scriptFile);
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
        final ScriptableSource other = (ScriptableSource) obj;
        if (!Objects.equals(this.charsetName, other.charsetName)) {
            return false;
        }
        if (!Objects.equals(this.inputFile, other.inputFile)) {
            return false;
        }
        if (!Objects.equals(this.scriptFile, other.scriptFile)) {
            return false;
        }
        return true;
    }

}
