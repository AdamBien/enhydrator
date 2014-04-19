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

import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.out.JDBCSink;
import com.airhacks.enhydrator.out.JDBCSinkTest;
import com.airhacks.enhydrator.out.Sink;
import java.util.List;
import java.util.logging.Logger;
import javax.persistence.Persistence;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

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
        this.sink = JDBCSinkTest.getSink();

    }

    @Test
    public void plainCopy() {
        CoffeeTestFixture.insertCoffee("arabica", 2, "hawai", Roast.LIGHT, "nice", "whole");
        CoffeeTestFixture.insertCoffee("niceone", 3, "russia", Roast.MEDIUM, "awful", "java beans");
        Pump pump = new Pump.Engine().
                flowListener(l -> Logger.getLogger("plainCopy").info(l)).
                from(this.source).
                to(this.sink).
                sqlQuery("select * from Coffee").
                build();
        pump.start();
        List<DeveloperDrink> all = CoffeeTestFixture.all();
        assertThat(all.size(), is(2));
    }

    @After
    public void cleanupTables() {
        CoffeeTestFixture.deleteTables();
    }

}
