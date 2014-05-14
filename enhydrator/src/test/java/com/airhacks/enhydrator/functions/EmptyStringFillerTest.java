package com.airhacks.enhydrator.functions;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class EmptyStringFillerTest {

    @Test
    public void fill() {
        EmptyStringFiller filler = new EmptyStringFiller();
        Row input = new Row();
        input.addNullColumn(0, "name");
        assertTrue(input.getColumnByIndex(0).isNullValue());
        Row output = filler.execute(input);
        assertFalse(output.getColumnByIndex(0).isNullValue());
    }

    @Test
    public void nullRow() {
        EmptyStringFiller filler = new EmptyStringFiller();
        Row output = filler.execute(null);
        assertNull(output);
    }

}
