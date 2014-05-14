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
import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class DestinationMapperTest {

    DestinationMapper cut;

    @Before
    public void init() {
        this.cut = new DestinationMapper();
    }

    @Test
    public void mapping() {
        Row input = new Row();
        final int INDEX = 0;
        input.addColumn(INDEX, "name", "duke");
        final String expectedSink = "customSink";
        final String expectedObject = "targetObject";
        this.cut.addMapping(INDEX, new TargetMapping(expectedSink, expectedObject));
        Row output = this.cut.execute(input);
        Column column = output.getColumnByIndex(INDEX);
        assertThat(column.getTargetObject(), is(expectedObject));
        assertThat(column.getTargetSink(), is(expectedSink));
    }

    @Test
    public void mapNotExisting() {
        Row input = new Row();
        final int INDEX = 0;
        int DOES_NOT_EXIST = 42;
        input.addColumn(INDEX, "name", "duke");
        final String expectedSink = "customSink";
        final String expectedObject = "targetObject";
        this.cut.addMapping(DOES_NOT_EXIST, new TargetMapping(expectedSink, expectedObject));
        Row output = this.cut.execute(input);
        assertNotNull(output);
    }

}
