package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Entry;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

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
        String expression = "print(current); print(columns);";
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
        List<Entry> execute = this.cut.execute(input, e1, "");
        assertTrue(execute.isEmpty());
    }

}
