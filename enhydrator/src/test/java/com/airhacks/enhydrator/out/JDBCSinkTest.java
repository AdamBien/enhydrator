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
        this.cut = new JDBCSink(null, null, null, null, TABLE_NAME);
    }

    @Test
    public void generateInsertStatement() {
        String expected = "INSERT INTO TARGET_TABLE VALUES (java,tengah)";
        List<Entry> row = new ArrayList<>();
        row.add(new Entry(0, "aName", 42, "java"));
        row.add(new Entry(1, "aName", 21, "tengah"));
        String actual = this.cut.generateInsertStatement(row);
        assertThat(actual, is(expected));
    }

}
