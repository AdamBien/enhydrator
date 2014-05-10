package com.airhacks.enhydrator.flexpipe;

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
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.in.JDBCSourceIT;
import com.airhacks.enhydrator.out.JDBCSinkTest;
import com.airhacks.enhydrator.out.Sink;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class PipelineTest {

    @Test
    public void jaxbSerialization() {
        Pipeline origin = getPipeline();
        Plumber plumber = new Plumber(".", "config");
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertEquals(deserialized, origin);
    }

    public static Pipeline getPipeline() {
        JDBCSource source = JDBCSourceIT.getSource();
        Sink sink = JDBCSinkTest.getSink();
        ColumnTranformation e1 = new ColumnTranformation("name", "convert", true);
        ColumnTranformation e2 = new ColumnTranformation(42, "compress", true);
        Pipeline origin = new Pipeline("tst", "src/test/scripts", "select * from Coffee where name like ? and strength = ?", source);
        origin.addSink(sink);
        origin.addQueryParam("arabica");
        origin.addQueryParam(2);
        origin.addEntryTransformation(e1);
        origin.addEntryTransformation(e2);
        origin.addPreRowTransformation("validate");
        origin.addPostRowTransformation("compress");
        origin.addPostRowTransformation("encrypt");
        origin.addFilter("true");
        origin.addExpression("print($ROW); $ROW");
        return origin;
    }

}
