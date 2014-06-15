package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Row;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class RowSinkTest {

    private Row expected;

    private boolean tested;

    @Before
    public void init() {
        this.expected = TestRows.getStringRow();
    }

    @Test
    public void process() {
        RowSink cut = new RowSink("*", r -> {
            assertThat(r, is(expected));
            this.setTested();
        });
        cut.processRow(expected);
        assertTrue(tested);
    }

    public void setTested() {
        this.tested = true;
    }

}
