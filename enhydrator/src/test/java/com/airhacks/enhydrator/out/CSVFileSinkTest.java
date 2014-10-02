package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.Row;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class CSVFileSinkTest {

    static final String FILE_NAME = "./target/temp" + System.currentTimeMillis() + ".csv";
    private static final String DELIMITER = "|";
    private final boolean USE_HEADERS = true;
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
        read.getColumnNames().stream().forEach(t
                -> assertTrue(entries.getColumnNames().contains(t)));
        Row content = result.iterator().next();
        assertNotNull(content);
    }

    @Test
    public void serializeLineWithoutHeaders() {
        CSVFileSink cut = new CSVFileSink(FILE_NAME, "|", false, false);
        cut.init();
        cut.processRow(getEntries());
    }

    Row getEntries() {
        Row row = new Row();
        row.addColumn(-1, "a", "java");
        row.addColumn(-1, "b", "tengah");
        return row;
    }

}
