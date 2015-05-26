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
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 *
 * @author airhacks.com
 */
public class FileFunctionScriptLoader extends FunctionScriptLoader {

    public FileFunctionScriptLoader(String baseFolder, Map<String, Object> scriptEngineBindings) {
        super(baseFolder, scriptEngineBindings);
    }

    @Override
    public Reader load(String scriptFolder, String name) {
        String fileName = name + ".js";
        try {
            return Files.newBufferedReader(Paths.get(baseFolder, scriptFolder, fileName), Charset.forName("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException("Encoding is not supported");
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot load script " + ex.getMessage());
        }
    }

}
