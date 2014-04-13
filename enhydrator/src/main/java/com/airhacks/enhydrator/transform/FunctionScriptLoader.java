package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;
import javax.script.Bindings;
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

    public Script getEntryTransformer() {
        Invocable invocable = (Invocable) engine;
        Object function;
        try {
            engine.eval("function execute(entry,list){ list.add(entry); return list; }");
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }

        return invocable.getInterface(Script.class);
    }

}
