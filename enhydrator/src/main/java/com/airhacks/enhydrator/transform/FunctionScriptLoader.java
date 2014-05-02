package com.airhacks.enhydrator.transform;

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
import com.airhacks.enhydrator.in.Row;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * @author airhacks.com
 */
public class FunctionScriptLoader {

    private final ScriptEngineManager manager;
    private final ScriptEngine engine;
    private String baseFolder;
    public static final String COLUMN_SCRIPT_FOLDER = "column";
    public static final String ROW_SCRIPT_FOLDER = "row";

    public FunctionScriptLoader(String baseFolder) {
        this();
        this.baseFolder = baseFolder;
    }

    public FunctionScriptLoader() {
        this.manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("nashorn");
        this.baseFolder = ".";
    }

    public EntryTransformer getEntryTransformer(String scriptName) {
        String content = load(COLUMN_SCRIPT_FOLDER, scriptName);
        Invocable invocable = (Invocable) engine;
        try {
            engine.eval(content);
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }
        return invocable.getInterface(EntryTransformer.class);
    }

    public RowTransformer getRowTransformer(String scriptName) {

        String content = load(ROW_SCRIPT_FOLDER, scriptName);
        Invocable invocable = (Invocable) engine;
        try {
            engine.eval(content);
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }
        RowTransformer scriptedTransformer = invocable.getInterface(RowTransformer.class);

        return (Row input) -> {
            try {
                return scriptedTransformer.execute(input);
            } catch (RuntimeException e) {
                throw new IllegalStateException("Cannot execute: " + scriptName, e);
            }
        };
    }

    public String load(String scriptFolder, String name) {
        String fileName = name + ".js";
        try {
            return new String(Files.readAllBytes(Paths.get(baseFolder, scriptFolder, fileName)), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException("Encoding is not supported");
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot load script " + ex.getMessage());
        }
    }
}
