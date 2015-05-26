package com.airhacks.enhydrator.transform;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 - 2015 Adam Bien
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

import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author ZeTo
 */
public class IndexMapperTest {

    IndexMapper indexMapper;

    Row row;

    public IndexMapperTest() {
        indexMapper = new IndexMapper();
    }

    @Before
    public void setUp() {
        row = new Row();
        row.addColumn(-1, "height", "tall");
        row.addColumn(-1, "age", "20");
        row.addColumn(-1, "name", "duke");
        row.addColumn(-1, "weight", "light-weight");
        row.addColumn(1, "resetIndexOne", "resetIndex");
        row.addColumn(2, "resetIndexTwo", "resetIndex");
    }

    /**
     * Test of execute method, of class IndexMapper.
     */
    @Test
    public void testExecuteReIndexColumns() {

        // define the index of columns by the ordered names array list
        indexMapper.orderedNames = new ArrayList<String>() {{ add("name"); add("age"); add("weight"); add("height");}};

        // do the test
        Row actualRow = indexMapper.execute(row);

        // check the indices
        assertEquals(0, actualRow.getColumnByName("name").getIndex());
        assertEquals(1, actualRow.getColumnByName("age").getIndex());
        assertEquals(2, actualRow.getColumnByName("weight").getIndex());
        assertEquals(3, actualRow.getColumnByName("height").getIndex());
        assertEquals(-1, actualRow.getColumnByName("resetIndexOne").getIndex());
        assertEquals(-1, actualRow.getColumnByName("resetIndexTwo").getIndex());

        Column nameColumn = actualRow.getColumnByIndex(0);
        assertEquals(0, nameColumn.getIndex());
        Column ageColumn = actualRow.getColumnByIndex(1);
        assertEquals(1, ageColumn.getIndex());
        Column weightColumn = actualRow.getColumnByIndex(2);
        assertEquals(2, weightColumn.getIndex());
        Column heightColumn = actualRow.getColumnByIndex(3);
        assertEquals(3, heightColumn.getIndex());

        // index -1 has only one entry - it was overwritten during the reindex of the column by index map
        Column columnByIndex = actualRow.getColumnByIndex(-1);
    }

}
