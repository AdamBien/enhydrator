/*
 * Copyright 2014 Adam Bien.
 *
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
 */
package com.airhacks.enhydrator.in;

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
import java.util.Iterator;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class CSVSourceTest {

    CSVSource cut;

    @Before
    public void init() {
        this.cut = new CSVSource("./src/test/files/cars.csv", ";", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fileDoesNotExist() {
        new CSVSource("does/NOT/exist", ";", true);
    }

    @Test
    public void query() {
        Iterable<Row> cars = this.cut.query(null);
        int counter = 0;
        for (Row row : cars) {
            counter++;
            assertThat(row.getNumberOfColumns(), is(4));
            System.out.println("List: " + row);
        }
        //Header is included
        assertThat(counter, is(4));
    }

    @Test
    public void columnNameIsSet() {
        Iterable<Row> cars = this.cut.query(null);
        int counter = 0;
        for (Row list : cars) {
            Object entry = list.getColumnValue("Year");
            //Year;Make;Model;Length
            counter++;
            assertNotNull(entry);

        }
        //Header is included
        assertThat(counter, is(4));
    }

    @Test
    public void nullHandling() {
        Source vss = getSource("./src/test/files/nullcolumns.csv");
        Iterable<Row> query = vss.query(null);
        Iterator<Row> iterator = query.iterator();
        iterator.next(); //skipping header
        Row first = iterator.next();
        assertNotNull(first);
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

    @Test
    public void splitEmtpyRows() {
        String[] split = CSVSource.split(";;", ";");
        assertNotNull(split);
        assertThat(split.length, is(3));
        for (String column : split) {
            assertTrue(column.isEmpty());
        }
    }

    @Test
    public void splitEmptyStrings() {
        String[] split = CSVSource.split(" ; ; ", ";");
        assertNotNull(split);
        assertThat(split.length, is(3));
        for (String column : split) {
            assertThat(column, is(" "));
        }
    }

    @Test
    public void pyramid() {
        Source vss = getSource("./src/test/files/pyramid.csv");
        Iterable<Row> query = vss.query(null);
        int counter = 1;
        boolean readHeader = false;
        String columnName;
        for (Row list : query) {
            if (!readHeader) {
                readHeader = true;
                continue;
            }
            assertThat(list.getNumberOfColumns(), is(counter++));
            columnName = String.valueOf(counter - 1);
            assertThat(list.getColumnValue(columnName), is(columnName));
            assertNull(list.getColumnValue(String.valueOf(42)));
        }
    }

    Source getSource(final String fileName) {
        return new CSVSource(fileName, ";", true);
    }

}
