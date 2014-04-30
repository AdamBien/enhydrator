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
import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ExpressionTest {

    Expression cut;

    @Before
    public void init() {
        this.cut = new Expression();
    }

    @Test
    public void bindingsAreWorking() {
        String expression = "print(current); print(columns);java.util.Collections.EMPTY_LIST";
        Entry e1 = new Entry(0, "chief", "duke");
        Entry e2 = new Entry(1, "master", "juggy");
        List<Entry> input = new ArrayList<>();
        input.add(e1);
        input.add(e2);
        List<Entry> execute = this.cut.execute(input, e1, expression);
        assertTrue(execute.isEmpty());
    }

    @Test
    public void emptyList() {
        Entry e1 = new Entry(0, "chief", "duke");
        List<Entry> input = new ArrayList<>();
        List<Entry> execute = this.cut.execute(input, e1, "java.util.Collections.EMPTY_LIST");
        assertTrue(execute.isEmpty());
    }

    @Test
    public void emptyExpression() {
        Entry e1 = new Entry(0, "chief", "duke");
        List<Entry> input = new ArrayList<>();
        List<Entry> result = this.cut.execute(input, e1, "");
        assertThat(result, is(e1.asList()));
    }

}
