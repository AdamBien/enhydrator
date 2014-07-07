package com.airhacks.enhydrator.scenarios;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.LogSink;
import java.util.Iterator;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
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
        VirtualSinkSource vss = getSource("./src/test/files/cars.csv");

        int numberOfRows = vss.getNumberOfRows();
        assertTrue(numberOfRows > 0);
        Iterable<Row> query = vss.query(null);
        boolean foundFord = false;
        for (Row list : query) {
            assertThat(list.getNumberOfColumns(), is(4));
            if ("Ford".equals(list.getColumnValue("Make"))) {
                foundFord = true;
            }
        }
        assertTrue(foundFord);
    }

    @Test
    public void nullHandling() {
        VirtualSinkSource vss = getSource("./src/test/files/nullcolumns.csv");
        int numberOfRows = vss.getNumberOfRows();
        assertThat(numberOfRows, is(3));
        Iterable<Row> query = vss.query(null);
        Iterator<Row> iterator = query.iterator();
        iterator.next(); //skipping header
        Row first = iterator.next();
        assertNull(first.getColumnValue("1"));
        assertNull(first.getColumnValue("2"));
        assertNull(first.getColumnValue("3"));
        assertNull(first.getColumnValue("4"));

        Row second = iterator.next();
        String emptyString = " ";
        assertThat(second.getColumnValue("1"), is(emptyString));
        assertThat(second.getColumnValue("2"), is(emptyString));
        assertThat(second.getColumnValue("3"), is(emptyString));
        assertNull(second.getColumnValue("4"));

    }

    VirtualSinkSource getSource(final String fileName) {
        Source source = new CSVFileSource(fileName, ";", "UTF-8", true);
        VirtualSinkSource vss = new VirtualSinkSource();
        Pump pump = new Pump.Engine().
                from(source).
                to(vss).
                to(new LogSink()).
                build();
        pump.start();
        return vss;
    }
}
