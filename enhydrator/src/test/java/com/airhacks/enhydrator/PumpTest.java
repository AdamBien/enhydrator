package com.airhacks.enhydrator;

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

import com.airhacks.enhydrator.flexpipe.JDBCPipeline;
import com.airhacks.enhydrator.flexpipe.JDBCPipelineTest;
import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.CoreMatchers;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author airhacks.com
 */
public class PumpTest {

    JDBCSource source;

    @Before
    public void initialize() {
        this.source = new JDBCSource.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./coffees;create=true").
                newSource();
    }

    @Test
    public void oneToOneTransformationWithName() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        Pump pump = new Pump.Engine().
                from(source).
                with("name", t -> t.asList()).
                to(consumer).sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));
    }

    @Test
    public void oneToOneTransformationWithIndex() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        Pump pump = new Pump.Engine().
                from(source).
                with(1, t -> t.changeValue("duke").asList()).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));
    }

    @Test
    public void ignoringPreprocessor() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        final ArrayList<Entry> entries = new ArrayList<>();
        Pump pump = new Pump.Engine().
                from(source).
                startWith(l -> entries).
                sqlQuery("select * from Coffee").
                to(consumer).
                build();
        pump.start();
        verify(consumer, times(2)).processRow(entries);
    }

    @Test
    public void postPreprocessor() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        final ArrayList<Entry> entries = new ArrayList<>();
        Pump pump = new Pump.Engine().
                from(source).
                endWith(l -> entries).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(entries);
    }

    @Test
    public void passThrough() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        Pump pump = new Pump.Engine().
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));
    }

    @Test
    public void scriptEntryTransformer() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        Pump pump = new Pump.Engine().
                homeScriptFolder("./src/test/scripts").
                from(source).
                with(1, "quote").
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));

    }

    @Test
    public void scriptRowTransformer() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        Pump pump = new Pump.Engine().
                homeScriptFolder("./src/test/scripts").
                startWith("reverse").
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(List.class));

    }

    @Test
    public void usePipeline() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        JDBCPipeline pipeline = JDBCPipelineTest.getJDBCPipeline();
        Sink consumer = mock(Sink.class);
        Pump pump = new Pump.Engine().
                flowListener(l -> System.out.println(l)).
                use(pipeline).
                to(consumer).
                build();
        pump.start();
        verify(consumer).processRow(any(List.class));
    }

    @Test
    public void usePipelineWithSink() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        JDBCPipeline pipeline = JDBCPipelineTest.getJDBCPipeline();
        Pump pump = new Pump.Engine().
                flowListener(l -> System.out.println(l)).
                use(pipeline).
                build();
        pump.start();
    }

    @Test
    public void applyExpressionsWithoutExpressions() {
        Pump pump = new Pump.Engine().
                build();
        List<Entry> entries = getEntries();
        final Entry expected = entries.get(0);
        List<Entry> result = pump.applyExpressions(entries, expected);
        assertThat(result, CoreMatchers.hasItem(expected));
        assertThat(result.size(), is(1));
    }

    List<Entry> getEntries() {
        List<Entry> row = new ArrayList<>();
        row.add(new Entry(0, "a", 42, "java"));
        row.add(new Entry(1, "b", 21, "tengah"));
        return row;
    }

    @After
    public void clearTables() {
        CoffeeTestFixture.deleteTables();
    }

}
