package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.Collections;
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
        Entry e1 = new Entry(0, "chief", 42, "duke");
        Entry e2 = new Entry(1, "master", 21, "juggy");
        List<Entry> input = new ArrayList<>();
        input.add(e1);
        input.add(e2);
        List<Entry> execute = this.cut.execute(input, e1, expression);
        assertTrue(execute.isEmpty());
    }

    @Test
    public void emptyList() {
        Entry e1 = new Entry(0, "chief", 42, "duke");
        List<Entry> input = new ArrayList<>();
        List<Entry> execute = this.cut.execute(input, e1, "java.util.Collections.EMPTY_LIST");
        assertTrue(execute.isEmpty());
    }

    @Test
    public void emptyExpression() {
        Entry e1 = new Entry(0, "chief", 42, "duke");
        List<Entry> input = new ArrayList<>();
        List<Entry> result = this.cut.execute(input, e1, "");
        assertThat(result, is(e1.asList()));
    }

}
