package com.airhacks.enhydrator.out;

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
public class JDBCSinkTest {

    JDBCSink cut;
    static final String TABLE_NAME = "TARGET_TABLE";

    @Before
    public void init() {
        this.cut = getSink();
    }

    public static JDBCSink getSink() {
        return new JDBCSink("aDriver", "localhost", "duke", "s3cret", TABLE_NAME);
    }

    @Test
    public void generateInsertStatement() {
        String expected = "INSERT INTO TARGET_TABLE (a,b) VALUES (java,tengah)";
        List<Entry> row = new ArrayList<>();
        row.add(new Entry(0, "a", 42, "java"));
        row.add(new Entry(1, "b", 21, "tengah"));
        String actual = this.cut.generateInsertStatement(row);
        assertThat(actual, is(expected));
    }

}
