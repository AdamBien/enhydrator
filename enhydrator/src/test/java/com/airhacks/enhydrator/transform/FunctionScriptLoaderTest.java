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
import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class FunctionScriptLoaderTest {

    FunctionScriptLoader cut;
    public static final String SCRIPTS_HOME_FOLDER = "./src/test/scripts";
    private Map<String, Object> scriptEngineBindings;

    @Before
    public void init() {
        this.scriptEngineBindings = new HashMap<>();
        this.cut = new FileFunctionScriptLoader(SCRIPTS_HOME_FOLDER, this.scriptEngineBindings);

    }

    @Test
    public void entryTransfomerLoadingAndExecution() throws Exception {
        ColumnTransformer function = this.cut.getColumnTransformer("noop");
        assertNotNull(function);
        final String input = "chief";
        Object result = function.execute(input);
        assertThat(result, is(input));
    }

    @Test(expected = NullPointerException.class)
    public void createWithNullParameter() {
        FunctionScriptLoader.create(null, null);
    }

    @Test
    public void createResourceFunctionScriptLoader() {
        FunctionScriptLoader actual = FunctionScriptLoader.create("resource___anything", null);
        assertNotNull(actual);
        assertTrue(actual instanceof ResourceFunctionScriptLoader);
    }

    @Test
    public void createFileFunctionScriptLoader() {
        FunctionScriptLoader actual = FunctionScriptLoader.create("/config", null);
        assertTrue(actual instanceof FileFunctionScriptLoader);
    }

    @Test
    public void rowTransfomerLoadingAndExecution() throws Exception {
        Row input = new Row();
        input.addColumn(-1, "chief", "duke");
        RowTransformer function = this.cut.getRowTransformer("noop");
        assertNotNull(function);
        Row transformedEntries = function.execute(input);
        assertThat(transformedEntries, is(input));
    }

    @Test
    public void nullInputIsIgnored() {
        RowTransformer rowTransformer = this.cut.getRowTransformer("does-not-matter");
        Row output = rowTransformer.execute(null);
        assertNull(output);
    }

    @Test
    public void load() throws IOException {
        Reader content = this.cut.load("column", "noop");
        assertNotNull(content);
        assertTrue(content.read() != -1);
    }

    @Test
    public void bindingsAvailable() {
        Row input = new Row();
        final String inputValue = "duke";
        input.addColumn(-1, "name", inputValue);
        RowTransformer function = this.cut.getRowTransformer("bindings");
        Row output = function.execute(input);
        assertNotNull(output);
        final Column outputColumn = output.getColumnByName("synthetic");
        assertNotNull(outputColumn);
        assertThat(outputColumn.getValue(), is(inputValue));
    }

    @Test
    public void counterIsWorking() {
        Row input = new Row();
        input.useMemory(new Memory());
        final String inputValue = "duke";
        input.addColumn(-1, "name", inputValue);
        RowTransformer function = this.cut.getRowTransformer("count");
        Row output = function.execute(input);
        assertNotNull(output);
        Memory memory = output.getMemory();
        assertNotNull(memory);
        assertThat(memory.counterValue(), is(1l));
    }

    @Test
    public void propertyIsStoredInMemory() {
        Row input = new Row();
        input.useMemory(new Memory());
        final String inputValue = "duke";
        input.addColumn(-1, "name", inputValue);
        RowTransformer function = this.cut.getRowTransformer("store");
        Row output = function.execute(input);
        Memory memory = output.getMemory();
        assertThat(memory.get("from"), is("script"));
    }

}
