package com.airhacks.enhydrator;

import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.JDBCSink;
import com.airhacks.enhydrator.out.Sink;
import java.util.List;
import javax.persistence.Persistence;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
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
public class CopyTableTest {

    JDBCSource source;
    Sink sink;

    @Before
    public void initialize() {
        Persistence.generateSchema("to", null);
        Persistence.generateSchema("from", null);
        this.source = new JDBCSource.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./coffees;create=true").
                newSource();
        this.sink = new JDBCSink.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./targetDB;create=true").
                targetTable("DEVELOPER_DRINK").
                newSink();

    }

    @Test
    public void plainCopy() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Pump pump = new Pump.Engine().
                from(this.source).
                to(this.sink).build();
        pump.start("select * from Coffee");
        List<DeveloperDrink> all = CoffeeTestFixture.all();
        assertThat(all.size(), is(2));
    }

    @After
    public void cleanupTables() {
        CoffeeTestFixture.deleteTables();
    }

}
