package com.airhacks.enhydrator.transform;

import java.io.File;
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
        String content = load("entry", scriptName);
        Invocable invocable = (Invocable) engine;
        try {
            engine.eval(content);
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }
        return invocable.getInterface(EntryTransformer.class);
    }

    public RowTransformer getRowTransformer(String scriptName) {
        String content = load("row", scriptName);
        Invocable invocable = (Invocable) engine;
        try {
            engine.eval(content);
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }
        return invocable.getInterface(RowTransformer.class);
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
