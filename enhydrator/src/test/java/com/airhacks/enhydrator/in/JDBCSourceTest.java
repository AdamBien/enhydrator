package com.airhacks.enhydrator.in;

import com.airhacks.enhydrator.CoffeeTestFixture;
import com.airhacks.enhydrator.Roast;
import com.airhacks.enhydrator.transform.ResultSetToEntries;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class JDBCSourceTest {

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
        Iterable<ResultSet> result = getSource().query("select * from Coffee");
        boolean iterated = false;
        for (ResultSet resultSet : result) {
            fail("There should be no data");
        }
        assertNotNull(result);
        assertFalse(iterated);
    }

    @Test
    public void queryExecution() throws SQLException {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        Iterable<ResultSet> result = getSource().query("select * from Coffee");
        boolean iterated = false;
        for (ResultSet resultSet : result) {
            iterated = true;
            Object object = resultSet.getObject(1);
            assertNotNull(object);
            System.out.println("First row: " + object);
        }
        assertNotNull(result);
        assertTrue(iterated);
    }

    @Test
    public void queryExecutionWithParameters() throws SQLException {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        CoffeeTestFixture.insertCoffee("espresso", 42, "tengah", Roast.DARK, "good", "whole");
        Iterable<ResultSet> result = getSource().query("select * from Coffee where name like ?", "java");
        boolean iterated = false;
        int counter = 0;
        for (ResultSet resultSet : result) {
            iterated = true;
            Object object = resultSet.getObject(1);
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
                    try {
                        System.out.println(t.getObject(1));
                    } catch (SQLException ex) {
                        Logger.getLogger(JDBCSource.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });
    }

    @Test
    public void convertToEntry() {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        StreamSupport.
                stream(getSource().query("select * from Coffee").
                        spliterator(), false).
                map(new ResultSetToEntries()).
                forEach(t -> System.out.println(t));

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
