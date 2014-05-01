package com.airhacks.enhydrator.in;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class RowTest {

    Row cut;

    @Before
    public void init() {
        this.cut = new Row(0);
    }

    @Test
    public void addString() {
        String expected = "duke";
        Object actual = this.cut.addColumn("name", expected).getColumn("name");
        assertThat(actual, is(expected));
    }

    public void addLong() {
        long expected = 42;
        final String column = "answer";
        Object actual = this.cut.addColumn(column, expected).getColumn(column);
        assertThat(actual, is(expected));

    }

    public void addAndRemoveColumn() {
        int expected = 42;
        final String column = "answer";
        Object actual = this.cut.addColumn(column, expected).getColumn(column);
        assertNotNull(actual);
        this.cut.removeColumn(column);
        actual = this.cut.getColumn(column);
        assertNull(actual);

    }

}
