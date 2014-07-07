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
import java.io.Reader;
import java.util.Objects;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * @author airhacks.com
 */
public abstract class FunctionScriptLoader {

    public static FunctionScriptLoader create(String baseFolder) {
        Objects.requireNonNull(baseFolder, "Parameter baseFolder cannot be null!");
        if (baseFolder.startsWith("resource")) {
            return new ResourceFunctionScriptLoader();
        } else {
            return new FileFunctionScriptLoader(baseFolder);
        }
    }

    private final ScriptEngineManager manager;
    private final ScriptEngine engine;
    protected String baseFolder;
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

    public ColumnTransformer getColumnTransformer(String scriptName) {
        Reader content = load(COLUMN_SCRIPT_FOLDER, scriptName);
        Invocable invocable = (Invocable) engine;
        try {
            engine.eval(content);
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }
        return invocable.getInterface(ColumnTransformer.class);
    }

    public RowTransformer getRowTransformer(String scriptName) {
        return (Row input) -> {
            if (input == null) {
                return null;
            }
            Reader content = load(ROW_SCRIPT_FOLDER, scriptName);
            try {
                Bindings bindings = ScriptingEnvironmentProvider.create(manager, input);
                return (Row) engine.eval(content, bindings);
            } catch (ScriptException ex) {
                throw new IllegalStateException("Cannot evaluate script: " + scriptName, ex);
            }
        };
    }

    public abstract Reader load(String scriptFolder, String name);
}
