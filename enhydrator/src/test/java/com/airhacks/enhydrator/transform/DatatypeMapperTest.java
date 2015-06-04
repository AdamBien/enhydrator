package com.airhacks.enhydrator.transform;

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

import com.airhacks.enhydrator.in.Row;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class DatatypeMapperTest {

    DatatypeIndexMapper cut;

    @Before
    public void init() {
        this.cut = new DatatypeIndexMapper();
    }

    @Test
    public void doubleMapping() {
        Row input = getRow();
        this.cut.addMapping(0, Datatype.DOUBLE);
        Row output = this.cut.execute(input);
        assertTrue(output.getColumnByIndex(0).getValue() instanceof Double);
    }

    @Test
    public void integerMapping() {
        Row input = getRow();
        this.cut.addMapping(0, Datatype.INTEGER);
        Row output = this.cut.execute(input);
        assertTrue(output.getColumnByIndex(0).getValue() instanceof Integer);
    }

    Row getRow() {
        Row row = new Row();
        return row.addColumn(0, "name", "42");
    }

}
