package com.airhacks.enhydrator.transform;

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

    public FunctionScriptLoader() {
        this.manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("nashorn");
    }

    public EntryTransformer getEntryTransformer() {
        Invocable invocable = (Invocable) engine;
        try {
            engine.eval("function execute(entry,list){ list.add(entry); return list; }");
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }

        return invocable.getInterface(EntryTransformer.class);
    }

}
