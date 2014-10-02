package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.Row;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 * @author airhacks.com
 */
@RunWith(Parameterized.class)
public class CSVFileSinkTest {

    static final String FILE_NAME = "./target/temp" + System.currentTimeMillis() + ".csv";
    private static final String DELIMITER = "|";
    public int input;

    @Parameterized.Parameter(0)
    public boolean USE_HEADERS = true;

    @Parameterized.Parameters(name = "Test with headers: {index} -> {0})")
    public static List<Boolean[]> data() {
        return Arrays.asList(new Boolean[][]{{true}, {false}});
    }
    private CSVFileSink cut;

    @Before
    public void init() {
        this.cut = new CSVFileSink(FILE_NAME, DELIMITER, USE_HEADERS, false);
    }

    @Test
    public void serializeLineWithHeaders() {
        cut.init();
        final Row entries = getEntries();
        cut.processRow(entries);
        cut.close();

        CSVFileSource source = new CSVFileSource(FILE_NAME, DELIMITER, "utf-8", USE_HEADERS);
        Iterable<Row> result = source.query(null, null);
        Row read = result.iterator().next();
        assertNotNull(read);
        System.out.println(entries.getColumnNames() + " " + read.getColumnNames());
        System.out.println(entries.getColumnValues().values() + " " + read.getColumnValues().values());
        if (USE_HEADERS) {
            read.getColumnNames().stream().forEach(t
                    -> assertTrue(entries.getColumnNames().contains(t)));
        } else {
            read.getColumnValues().values().stream().forEach(t
                    -> assertTrue(entries.getColumnValues().values().contains(t)));
        }
    }

    Row getEntries() {
        Row row = new Row();
        row.addColumn(-1, "a", "java");
        row.addColumn(-1, "b", "tengah");
        return row;
    }

}
