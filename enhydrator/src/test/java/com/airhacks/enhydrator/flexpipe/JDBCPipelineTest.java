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
import com.airhacks.enhydrator.in.JDBCSourceTest;
import com.airhacks.enhydrator.out.JDBCSinkTest;
import com.airhacks.enhydrator.out.Sink;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class JDBCPipelineTest {

    @Test
    public void jaxbSerialization() {
        JDBCPipeline origin = getJDBCPipeline();
        Plumber plumber = new Plumber(".", "config");
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertEquals(deserialized, origin);
    }

    public static JDBCPipeline getJDBCPipeline() {
        JDBCSource source = JDBCSourceTest.getSource();
        Sink sink = JDBCSinkTest.getSink();
        EntryTransformation e1 = new EntryTransformation("name", "convert", true);
        EntryTransformation e2 = new EntryTransformation(42, "compress", true);
        JDBCPipeline origin = new JDBCPipeline("tst", "./src/test/scripts", "select * from Coffee where name like ? and strength = ?", source, sink);
        origin.addQueryParam("arabica");
        origin.addQueryParam(2);
        origin.addEntryTransformation(e1);
        origin.addEntryTransformation(e2);
        origin.addPreRowTransforation("reverse");
        origin.addPreRowTransforation("validate");
        origin.addPostRowTransformation("compress");
        origin.addPostRowTransformation("encrypt");
        origin.addFilter("true");
        origin.addExpression("print(current); java.util.Collections.emptyList();");
        return origin;
    }

}
