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
import com.airhacks.enhydrator.flexpipe.Pipeline;
import com.airhacks.enhydrator.flexpipe.PipelineTest;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.out.NamedSink;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author airhacks.com
 */
public class PumpIT {

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
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                with("name", t -> t).
                to(consumer).sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(Row.class));
    }

    static NamedSink getMockedSink() {
        NamedSink mock = mock(NamedSink.class);
        when(mock.getName()).thenReturn("*");
        return mock;
    }

    @Test
    public void ignoringPreprocessor() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                startWith(l -> null).
                sqlQuery("select * from Coffee").
                to(consumer).
                build();
        pump.start();
        verify(consumer, never()).processRow(any(Row.class));
    }

    @Test
    public void postPreprocessor() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                endWith(l -> l).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(Row.class));
    }

    @Test
    public void passThrough() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").build();
        pump.start();
        verify(consumer, times(2)).processRow(any(Row.class));
    }

    @Test
    public void ignoringFilter() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                filter("false").
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").build();
        long rowCount = pump.start().getProcessedRowCount();
        //counts all rows, not processed rows
        assertThat(rowCount, is(2l));
        verify(consumer, never()).processRow(any(Row.class));
    }

    @Test
    public void acceptingFilter() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                filter("true").
                filter("$ROW.empty === false").
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        long rowCount = pump.start().getProcessedRowCount();
        assertThat(rowCount, is(2l));
        verify(consumer, times(2)).processRow(any(Row.class));
    }

    @Test
    public void scriptEntryTransformer() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                homeScriptFolder("./src/test/scripts").
                from(source).
                withColumnScript("Name", "quote").
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(Row.class));

    }

    @Test
    public void scriptRowTransformer() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                homeScriptFolder("./src/test/scripts").
                startWith("validate").
                from(source).
                to(consumer).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        verify(consumer, times(2)).processRow(any(Row.class));

    }

    @Test
    public void usePipeline() {
        Pipeline pipeline = PipelineTest.getCSVPipeline();
        NamedSink consumer = getMockedSink();
        Pump pump = new Pump.Engine().
                flowListener(l -> System.out.println(l)).
                use(pipeline).
                to(consumer).
                build();
        pump.start();
        verify(consumer, times(4)).processRow(any(Row.class));
    }

    @Test
    public void usePipelineWithSink() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Pipeline pipeline = PipelineTest.getJDBCPipeline();
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
        Row entries = getEntries();
        int expected = entries.getNumberOfColumns();
        pump.applyExpressions(entries);
        int actual = entries.getNumberOfColumns();
        assertThat(actual, is(expected));
    }

    @Test
    public void applyRowTransformationsWithoutFunctions() {
        Row input = getEntries();
        Row output = Pump.applyRowTransformations(new ArrayList<>(), input);
        assertThat(output, is(input));
    }

    @Test
    public void applyRowTransformationsWitDevNull() {
        Row input = getEntries();
        List<Function<Row, Row>> funcs = new ArrayList<>();
        funcs.add(l -> new Row());
        Row output = Pump.applyRowTransformations(funcs, input);
        assertTrue(output.isEmpty());
    }

    Row getEntries() {
        Row row = new Row();
        row.addColumn(0, "a", "java");
        row.addColumn(1, "b", "tengah");
        return row;
    }

    @After
    public void clearTables() {
        CoffeeTestFixture.deleteTables();
    }

}
