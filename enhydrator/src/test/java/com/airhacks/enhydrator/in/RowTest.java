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
import java.util.Map;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
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
        this.cut = new Row();
    }

    @Test
    public void addString() {
        String expected = "duke";
        Object actual = this.cut.addColumn(0, "name", expected).getColumnValue("name");
        assertThat(actual, is(expected));
    }

    @Test
    public void addLong() {
        long expected = 42;
        final String column = "answer";
        Object actual = this.cut.addColumn(0, column, expected).getColumnValue(column);
        assertThat(actual, is(expected));

    }

    @Test
    public void addAndRemoveColumn() {
        int expected = 42;
        final String column = "answer";
        Object actual = this.cut.addColumn(0, column, expected).getColumnValue(column);
        assertNotNull(actual);
        this.cut.removeColumn(column);
        actual = this.cut.getColumnValue(column);
        assertNull(actual);
    }

    @Test
    public void getColumnsGroupedByDefaultDestination() {
        this.cut.addColumn(0, "name", "duke");
        this.cut.addColumn(0, "city", "SFO");
        this.cut.changeDestination("name", "LA");
        Map<String, Row> grouped = this.cut.getColumnsGroupedByDestination();
        assertFalse(grouped.isEmpty());
        Row defaultDestination = grouped.get("*");
        assertNotNull(defaultDestination);
        assertThat(defaultDestination.getColumnValue("city"), is("SFO"));

        Row laDestination = grouped.get("LA");
        assertNotNull(laDestination);
        assertThat(laDestination.getColumnValue("name"), is("duke"));
    }

    @Test
    public void changeColumnName() {
        final String oldName = "duke";
        this.cut.addColumn(0, oldName, "--any value--");
        assertThat(this.cut.getNumberOfColumns(), is(1));
        final String newName = "juggy";
        this.cut.changeColumnName(oldName, newName);
        assertThat(this.cut.getNumberOfColumns(), is(1));
        Column columnByName = this.cut.getColumnByName(newName);
        assertThat(columnByName.getName(), is(newName));

        Column originColumn = this.cut.getColumnByName(oldName);
        assertNull(originColumn);

    }

    @Test
    public void getColumnsGroupedByDestination() {
        this.cut.addColumn(0, "name", "duke");
        this.cut.addColumn(0, "city", "SFO");
        this.cut.changeDestination("name", "LA");
        Map<String, Row> grouped = this.cut.getColumnsGroupedByDestination();
        assertThat(grouped.keySet().size(), is(2));
    }

}
