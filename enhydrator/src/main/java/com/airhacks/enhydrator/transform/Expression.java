package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
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
    private Consumer<String> expressionListener;

    public Expression() {
        this(l -> {
        });
    }

    public Expression(Consumer<String> expressionListener) {
        this.expressionListener = expressionListener;
        this.manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("nashorn");
    }

    public List<Entry> execute(List<Entry> columns, Entry entry, String expression) {
        Bindings bindings = this.engine.createBindings();
        bindings.put("columns", columns);
        bindings.put("current", entry);
        try {
            this.expressionListener.accept("Executing: " + expression);
            Object result = this.engine.eval(expression, bindings);
            this.expressionListener.accept("Got result: " + result);
            if (!(result instanceof List)) {
                return Collections.EMPTY_LIST;
            } else {
                return (List<Entry>) result;
            }
        } catch (ScriptException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }
}
