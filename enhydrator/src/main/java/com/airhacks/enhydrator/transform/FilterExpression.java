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
import java.util.function.Consumer;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 * @author airhacks.com
 */
public class FilterExpression {

    private final ScriptEngineManager manager;
    private final ScriptEngine engine;
    private Consumer<String> expressionListener;

    public FilterExpression() {
        this(l -> {
        });
    }

    public FilterExpression(Consumer<String> expressionListener) {
        this.expressionListener = expressionListener;
        this.manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("nashorn");
    }

    public Boolean execute(Row columns, String expression) {
        Bindings bindings = ScriptingEnvironmentProvider.create(this.manager, columns);
        try {
            this.expressionListener.accept("Executing: " + expression);
            Object result = this.engine.eval(expression, bindings);
            this.expressionListener.accept("Got result: " + result);
            if (!(result instanceof Boolean)) {
                return Boolean.FALSE;
            } else {
                return (Boolean) result;
            }
        } catch (ScriptException ex) {
            throw new IllegalStateException(ex.getMessage(), ex);
        }
    }
}
