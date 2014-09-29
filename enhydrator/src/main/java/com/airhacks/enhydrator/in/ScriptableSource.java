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
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 * @author airhacks.com
 */
public class ScriptableSource implements Source {

    private final String charsetName;
    private Charset charset;
    private List<Row> iterable;
    private final Path input;
    private ScriptEngine nashorn;
    private final Reader script;
    private List<Row> rows;

    public ScriptableSource(Path path, Reader script, String charset) {
        this.input = path;
        this.charsetName = charset;
        this.script = script;
        init();
    }

    void init() throws IllegalStateException, IllegalArgumentException {
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

}
