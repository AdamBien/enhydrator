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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class FunctionScriptLoaderTest {

    FunctionScriptLoader cut;
    public static final String SCRIPTS_HOME_FOLDER = "./src/test/scripts";

    @Before
    public void init() {
        this.cut = new FunctionScriptLoader(SCRIPTS_HOME_FOLDER);
    }

    @Test
    public void entryTransfomerLoadingAndExecution() throws Exception {
        EntryTransformer function = this.cut.getEntryTransformer("noop");
        assertNotNull(function);
        final String input = "chief";
        Object result = function.execute(input);
        assertThat(result, is(input));
    }

    @Test
    public void rowTransfomerLoadingAndExecution() throws Exception {
        Row input = new Row();
        input.addColumn("chief", "duke");
        RowTransformer function = this.cut.getRowTransformer("noop");
        assertNotNull(function);
        Row transformedEntries = function.execute(input);
        assertThat(transformedEntries, is(input));
    }

    @Test
    public void load() {
        String content = this.cut.load("column", "noop");
        assertNotNull(content);
    }

}
