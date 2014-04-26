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
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class VirtualSinkSourceTest {

    @Test
    public void creation() {
        VirtualSinkSource source = getSource();
        assertThat(source.getNumberOfRows(), is(2));
        List<Entry> first = source.getRow(0);
        assertThat(first.size(), is(2));
        List<Entry> second = source.getRow(1);
        assertThat(second.size(), is(2));
    }

    VirtualSinkSource getSource() {
        return new VirtualSinkSource.Rows().
                addColumn("age", "25").
                addColumn("master", "duke").
                addRow().
                addColumn("age", "15").
                addColumn("master", "juggy").
                build();
    }

    @Test
    public void numberOfRows() {
        VirtualSinkSource source = new VirtualSinkSource.Rows().
                addColumn("age", "25").
                addRow().
                addColumn("age", "15").
                addRow().
                addColumn(new Entry(0, "age", "42")).
                build();
        assertThat(source.getNumberOfRows(), is(3));
    }

    @Test
    public void numberOfColumns() {
        VirtualSinkSource source = new VirtualSinkSource.Rows().
                addColumn("age", "25").
                addColumn(new Entry(0, "age", "42")).
                addColumn("age", "15").
                build();
        List<Entry> row = source.getRow(0);
        assertThat(row.size(), is(3));
    }

    @Test
    public void processRow() {
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(0, "a", "b"));
        entries.add(new Entry(1, "c", "d"));
        VirtualSinkSource source = new VirtualSinkSource();
        source.processRow(entries);
        assertThat(source.getNumberOfRows(), is(1));
        List<Entry> actual = source.getRow(0);
        assertThat(actual, is(entries));
    }

    @Test
    public void query() {
        VirtualSinkSource source = getSource();
        Iterable<List<Entry>> results = source.query(null);
        int rowCounter = 0;
        for (List<Entry> list : results) {
            assertNotNull(list);
            assertThat(list.size(), is(2));
            rowCounter++;
        }
        assertThat(rowCounter, is(2));
    }

}
