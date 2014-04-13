package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.List;
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

    public Callable<List<Entry>> getEntryTransformer(Entry entry) {
        Bindings bindings = engine.createBindings();
        bindings.put("entry", entry);
        bindings.put("list", new ArrayList());
        Invocable invocable = (Invocable) engine;
        Object function;
        try {
            function = engine.eval("function run(){ list.add(entry); return list; }", bindings);
        } catch (ScriptException ex) {
            throw new IllegalStateException("Cannot evaluate script", ex);
        }

        return invocable.getInterface(function, Callable.class);
    }

}
