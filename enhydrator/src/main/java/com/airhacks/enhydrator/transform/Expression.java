package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 * @author airhacks.com
 */
public class Expression {

    private final ScriptEngineManager manager;
    private final ScriptEngine engine;

    public Expression() {
        this.manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("nashorn");
    }

    public List<Entry> execute(List<Entry> columns, Entry entry, String expression) {
        List<Entry> results = new ArrayList<>();
        Bindings bindings = this.engine.createBindings();
        bindings.put("columns", columns);
        bindings.put("current", entry);
        try {
            Object result = this.engine.eval(expression, bindings);
            if (!(result instanceof List)) {
                return Collections.EMPTY_LIST;
            }
        } catch (ScriptException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
        return results;
    }
}
