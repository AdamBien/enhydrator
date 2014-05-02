package com.airhacks.enhydrator.in;

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
import com.airhacks.enhydrator.CoffeeTestFixture;
import com.airhacks.enhydrator.Roast;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.stream.StreamSupport;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class JDBCSourceIT {

    /**
     * <property name="javax.persistence.jdbc.url"
     * value="jdbc:derby:./coffees;create=true"/>
     * <property name="javax.persistence.jdbc.driver"
     * value="org.apache.derby.jdbc.EmbeddedDriver"/>
     */
    @Test
    public void constructWithValidParameters() {
        JDBCSource cut = getSource();
        assertNotNull(cut);
    }

    public static JDBCSource getSource() {
        JDBCSource source = new JDBCSource.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./coffees;create=true").
                newSource();
        return source;
    }

    @Test(expected = IllegalStateException.class)
    public void constructInvalidJDBCSource() {
        JDBCSource cut = new JDBCSource.Configuration().
                driver("org.airhacks.driver").
                url("outer-space").
                newSource();
        assertNotNull(cut);
    }

    @Test
    public void queryExecutionWithEmptyTable() {
        Iterable<Row> result = getSource().query("select * from Coffee");
        boolean iterated = false;
        for (Row resultSet : result) {
            fail("There should be no data");
        }
        assertNotNull(result);
        assertFalse(iterated);
    }

    @Test
    public void queryExecution() throws SQLException {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        Iterable<Row> result = getSource().query("select * from Coffee");
        boolean iterated = false;
        for (Row resultSet : result) {
            iterated = true;
            Object object = resultSet.getColumn("NAME");
            assertNotNull(object);
        }
        assertNotNull(result);
        assertTrue(iterated);
    }

    @Test
    public void queryExecutionWithParameters() throws SQLException {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        CoffeeTestFixture.insertCoffee("espresso", 42, "tengah", Roast.DARK, "good", "whole");
        Iterable<Row> result = getSource().query("select * from Coffee where name like ?", "java");
        boolean iterated = false;
        int counter = 0;
        for (Row resultSet : result) {
            iterated = true;
            Object object = resultSet.getColumn("NAME");
            assertNotNull(object);
            counter++;
        }
        assertNotNull(result);
        assertTrue(iterated);
        assertThat(counter, is(1));
    }

    @Test
    public void stream() {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        StreamSupport.stream(getSource().query("select * from Coffee").spliterator(), false).
                forEach(t -> {
                    System.out.println(t.getColumn("name"));
                });
    }

    @Test
    public void jaxbSerialization() throws JAXBException, UnsupportedEncodingException {
        JAXBContext context = JAXBContext.newInstance(JDBCSource.class);
        Marshaller marshaller = context.createMarshaller();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final JDBCSource origin = getSource();
        marshaller.marshal(origin, baos);

        byte[] content = baos.toByteArray();
        System.out.println("Serialized: " + new String(content, "UTF-8"));
        ByteArrayInputStream bais = new ByteArrayInputStream(content);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        JDBCSource deserialized = (JDBCSource) unmarshaller.unmarshal(bais);
        assertNotNull(deserialized);

        assertNotSame(deserialized, origin);
        assertThat(deserialized, is(origin));

    }

    @After
    public void dropCoffee() {
        CoffeeTestFixture.deleteTables();
    }
}
