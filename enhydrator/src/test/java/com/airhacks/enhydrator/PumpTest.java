package com.airhacks.enhydrator;

import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.Sink;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
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
                to(consumer).build();
        pump.start("select * from Coffee");
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
                to(consumer).build();
        pump.start("select * from Coffee");
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
                to(consumer).build();
        pump.start("select * from Coffee");
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
                to(consumer).build();
        pump.start("select * from Coffee");
        verify(consumer, times(2)).processRow(entries);
    }

    @Test
    public void passThrough() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Sink consumer = mock(Sink.class);
        Pump pump = new Pump.Engine().
                from(source).
                to(consumer).build();
        pump.start("select * from Coffee");
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
                to(consumer).build();
        pump.start("select * from Coffee");
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
                build();
        pump.start("select * from Coffee");
        verify(consumer, times(2)).processRow(any(List.class));

    }

    @After
    public void clearTables() {
        CoffeeTestFixture.deleteTables();
    }

}
