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

import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.in.JDBCSourceIT;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.JDBCSinkTest;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.out.Sink;
import com.airhacks.enhydrator.transform.Datatype;
import com.airhacks.enhydrator.transform.DatatypeMapper;
import com.airhacks.enhydrator.transform.DestinationMapper;
import com.airhacks.enhydrator.transform.NashornRowTransformer;
import com.airhacks.enhydrator.transform.SkipFirstRow;
import com.airhacks.enhydrator.transform.TargetMapping;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class PipelineTest {

    @Test
    public void jaxbJDBCPipelineSerialization() {
        Pipeline origin = getJDBCPipeline();
        Plumber plumber = new Plumber(".", "config");
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertEquals(deserialized, origin);
    }

    @Test
    public void jaxbCSVPipelineSerialization() {
        Pipeline origin = getCSVPipeline();
        Plumber plumber = new Plumber(".", "config");
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertEquals(deserialized, origin);
    }

    public static Pipeline getJDBCPipeline() {
        DestinationMapper mapper = new DestinationMapper();
        mapper.addMapping(0, new TargetMapping("*", "*"));
        JDBCSource source = JDBCSourceIT.getSource();
        Sink logSink = new LogSink();
        Sink jdbcSink = JDBCSinkTest.getSink();
        ColumnTransformation e1 = new ColumnTransformation("name", "convert");
        ColumnTransformation e2 = new ColumnTransformation(42, "compress");
        Pipeline origin = new Pipeline("jdbc", "src/test/scripts", "select * from Coffee where name like ? and strength = ?", source);
        origin.addSink(logSink);
        origin.addSink(jdbcSink);
        origin.addQueryParam("arabica");
        origin.addQueryParam(2);
        origin.addEntryTransformation(e1);
        origin.addEntryTransformation(e2);
        origin.addPreRowTransformation(mapper);
        origin.addPostRowTransformation(new SkipFirstRow());
        origin.addPreRowTransformation(new NashornRowTransformer("src/test/scripts", "encrypt"));
        origin.addPostRowTransformation(new NashornRowTransformer("src/test/scripts", "compress"));
        origin.addFilter("true");
        origin.addExpression("print($ROW); $ROW");
        return origin;
    }

    public static Pipeline getCSVPipeline() {
        DestinationMapper targetMapper = new DestinationMapper();
        targetMapper.addMapping(0, new TargetMapping("*", "*"));
        DatatypeMapper datatypeMapper = new DatatypeMapper();
        datatypeMapper.addMapping(0, Datatype.DOUBLE);
        Source source = new CSVFileSource("./src/test/files/pyramid.csv", ";", "UTF-8", true);
        Sink logSink = new LogSink();
        Sink jdbcSink = new VirtualSinkSource();
        ColumnTransformation e1 = new ColumnTransformation("name", "convert");
        ColumnTransformation e2 = new ColumnTransformation(42, "compress");
        Pipeline origin = new Pipeline("csv", "src/test/scripts", "select * from Coffee where name like ? and strength = ?", source);
        origin.addSink(logSink);
        origin.addSink(jdbcSink);
        origin.addQueryParam("arabica");
        origin.addQueryParam(2);
        origin.addEntryTransformation(e1);
        origin.addEntryTransformation(e2);
        origin.addPreRowTransformation(targetMapper);
        origin.addPreRowTransformation(datatypeMapper);
        origin.addPreRowTransformation(new NashornRowTransformer("src/test/scripts", "encrypt"));
        origin.addPostRowTransformation(new NashornRowTransformer("src/test/scripts", "compress"));
        origin.addFilter("true");
        origin.addExpression("print($ROW); $ROW");
        return origin;
    }

}
