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
import javax.script.Bindings;
import javax.script.ScriptEngineManager;

/**
 *
 * @author airhacks.com
 */
public class ScriptingEnvironmentProvider {

    public static Bindings create(ScriptEngineManager scriptEngineManager, Row input) {
        Bindings bindings = scriptEngineManager.getBindings();
        bindings.put("$ROW", input);
        bindings.put("$EMPTY", new Row());
        bindings.put("$MEMORY", input.getMemory());
        input.getColumns().forEach(c -> bindings.put(c.getName(), c));
        return bindings;
    }
}
