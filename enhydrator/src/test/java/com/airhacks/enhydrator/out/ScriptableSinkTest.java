package com.airhacks.enhydrator.out;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 - 2015 Adam Bien
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
import java.io.IOException;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ScriptableSinkTest {

    @Test(expected = NullPointerException.class)
    public void nullScriptIsRecognized() {
        ScriptableSink sink = new ScriptableSink();
        sink.init();
    }

    @Test
    public void instantiate() throws IOException {
        try (ScriptableSink sink = new ScriptableSink("./src/test/scripts/sink.js")) {
            sink.init();
            sink.processRow(new Row());
        }
    }

}
