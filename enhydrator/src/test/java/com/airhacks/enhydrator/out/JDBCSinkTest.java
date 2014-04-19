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
        List<Entry> row = getEntries();
        String actual = this.cut.generateInsertStatement(row);
        assertThat(actual, is(expected));
    }

    List<Entry> getEntries() {
        List<Entry> row = new ArrayList<>();
        row.add(new Entry(0, "a", 42, "java"));
        row.add(new Entry(1, "b", 21, "tengah"));
        return row;
    }

    @Test
    public void columnList() {
        String expected = "a,b";
        String columns = JDBCSink.columnList(getEntries());
        assertThat(columns, is(expected));
    }

    @Test
    public void emptyColumnList() {
        String columns = JDBCSink.columnList(new ArrayList<>());
        assertNull(columns);
    }

    @Test
    public void valueList() {
        String expected = "java,tengah";
        String columns = JDBCSink.valueList(getEntries());
        assertThat(columns, is(expected));
    }

    @Test
    public void emptyValueList() {
        String columns = JDBCSink.valueList(new ArrayList<>());
        assertNull(columns);
    }

    @Test
    public void processEmptyRow() {
        this.cut.processRow(null);
        this.cut.processRow(new ArrayList<>());
    }

}
