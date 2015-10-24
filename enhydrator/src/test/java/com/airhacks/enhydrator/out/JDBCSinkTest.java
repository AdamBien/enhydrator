package com.airhacks.enhydrator.out;

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
import com.airhacks.enhydrator.db.UnmanagedConnectionProvider;
import com.airhacks.enhydrator.in.Row;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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
        this.cut = getDummySink();
    }

    public static JDBCSink getDummySink() {
        return new JDBCSink(new UnmanagedConnectionProvider("aDriver", "localhost", "duke", "s3cret"), TABLE_NAME);
    }

    public static NamedSink getSink() {
        return new JDBCSink.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./targetDB;create=true").
                targetTable("DEVELOPER_DRINK").
                name("*").
                newSink();
    }

    @Test
    public void generateInsertStatement() {
        String expected = "INSERT INTO TARGET_TABLE (a,b) VALUES ('java','tengah')";
        Row row = getEntries();
        String actual = this.cut.generateInsertStatement(row);
        assertThat(actual, is(expected));
    }

    Row getEntries() {
        Row row = new Row();
        row.addColumn(-1, "a", "java");
        row.addColumn(-1, "b", "tengah");
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
        String columns = JDBCSink.columnList(new Row());
        assertNull(columns);
    }

    @Test
    public void stringValueList() {
        String expected = "'java','tengah'";
        String columns = JDBCSink.valueList(getEntries());
        assertThat(columns, is(expected));
    }

    public void numberValueList() {
        String expected = "1,2";
        Row row = new Row();
        row.addColumn(-1, "a", 1);
        row.addColumn(-1, "b", 2);
        String columns = JDBCSink.valueList(row);
        assertThat(columns, is(expected));
    }

    @Test
    public void emptyValueList() {
        String columns = JDBCSink.valueList(new Row());
        assertNull(columns);
    }

    @Test
    public void processEmptyRow() {
        this.cut.processRow(null);
        this.cut.processRow(new Row());
    }

}
