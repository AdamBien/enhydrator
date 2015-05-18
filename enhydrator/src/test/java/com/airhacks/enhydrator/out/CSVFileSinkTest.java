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
import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.Column;
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
        this.cut = new CSVFileSink("*", FILE_NAME, DELIMITER, USE_HEADERS, false);
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
        Column c = new Column(-1, "c", null);
        row.addColumn(c);
        row.addColumn(-1, "d", "groovy");
        Column e = new Column(-1, "e", null);
        row.addColumn(e);
        return row;
    }

}
