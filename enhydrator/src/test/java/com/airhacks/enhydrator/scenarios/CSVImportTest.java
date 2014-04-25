package com.airhacks.enhydrator.scenarios;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.CSVSource;
import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.LogSink;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class CSVImportTest {

    /**
     * 1997;Ford;E350;2,34
     */
    @Test
    public void copy() {
        Source source = new CSVSource("./src/test/files/cars.csv", ";", true);
        VirtualSinkSource vss = new VirtualSinkSource();
        Pump pump = new Pump.Engine().
                from(source).
                to(vss).
                to(new LogSink()).
                build();
        pump.start();

        int numberOfRows = vss.getNumberOfRows();
        assertTrue(numberOfRows > 0);
        Iterable<List<Entry>> query = vss.query(null);
        boolean foundFord = false;
        for (List<Entry> list : query) {
            assertThat(list.size(), is(4));
            if ("Ford".equals(list.get(1).getValue())) {
                foundFord = true;
            }
        }
        assertTrue(foundFord);
    }

}
