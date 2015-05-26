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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.Objects;

/**
 *
 * @author airhacks.com
 */
public class ResourceFunctionScriptLoader extends FunctionScriptLoader {

    public ResourceFunctionScriptLoader(Map<String, Object> scriptEngineBindings) {
        super("/", scriptEngineBindings);
    }

    @Override
    public Reader load(String scriptFolder, String name) {
        String resourceName = this.baseFolder + scriptFolder + "/" + name + ".js";
        InputStream resourceAsStream = ResourceFunctionScriptLoader.class.getClassLoader().getResourceAsStream(resourceName);
        Objects.requireNonNull(resourceAsStream, "Cannot load function: " + name + " from: " + resourceName);
        return new InputStreamReader(resourceAsStream);
    }

}
