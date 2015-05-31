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
import java.util.Arrays;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ColumnCopierTest {

    private ColumnCopier cut;

    @Before
    public void init() {
        this.cut = new ColumnCopier();
    }

    @Test
    public void executeWithExistingMappings() {
        this.cut.columnMappings.put("duke", new ColumnCopier.NameList(Arrays.asList("java", "javaee")));
        Row row = new Row();
        row.addColumn(new Column(0, "duke", "42"));
        Row withCopiedColumns = this.cut.execute(row);
        assertFalse(withCopiedColumns.isEmpty());
        assertThat(withCopiedColumns.getColumns().size(), is(3));
        Column javaColumn = row.getColumnByName("java");
        assertNotNull(javaColumn);

        Column javaeeColumn = row.getColumnByName("javaee");
        assertNotNull(javaeeColumn);

    }

    @Test
    public void executeWithoutMappings() {
        Row row = new Row();
        row.addColumn(new Column(0, "duke", "42"));
        Row withCopiedColumns = this.cut.execute(row);
        assertFalse(withCopiedColumns.isEmpty());
        assertThat(withCopiedColumns.getColumns().size(), is(1));
    }

    @Test
    public void executeEmptyRowWithMappings() {
        this.cut.columnMappings.put("duke", new ColumnCopier.NameList(Arrays.asList("java", "javaee")));
        Row row = new Row();
        Row withCopiedColumns = this.cut.execute(row);
        assertTrue(withCopiedColumns.isEmpty());
    }

}
