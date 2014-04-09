package com.airhacks.enhydrator.in;

import com.airhacks.enhydrator.CoffeeTestFixture;
import com.airhacks.enhydrator.Roast;
import com.airhacks.enhydrator.transform.ResultSetStreamable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author airhacks.com
 */
public class SourceTest {

    /**
     * <property name="javax.persistence.jdbc.url"
     * value="jdbc:derby:./coffees;create=true"/>
     * <property name="javax.persistence.jdbc.driver"
     * value="org.apache.derby.jdbc.EmbeddedDriver"/>
     */
    @Test
    public void constructWithValidParameters() {
        Source cut = getSource();
        assertNotNull(cut);
    }

    Source getSource() {
        Source cut = new Source.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./coffees;create=true").
                newSource();
        return cut;
    }

    @Test(expected = IllegalStateException.class)
    public void constructInvalidSource() {
        Source cut = new Source.Configuration().
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
    public void stream() {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        StreamSupport.stream(getSource().query("select * from Coffee").spliterator(), false).
                forEach(t -> {
                    try {
                        System.out.println(t.getObject(1));
                    } catch (SQLException ex) {
                        Logger.getLogger(SourceTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });
    }

    @Test
    public void convertToEntry() {
        CoffeeTestFixture.insertCoffee("java", 42, "tengah", Roast.DARK, "good", "whole");
        StreamSupport.
                stream(getSource().query("select * from Coffee").
                        spliterator(), false).
                map(new ResultSetStreamable()).
                forEach(t -> System.out.println(t));

    }

    @After
    public void dropCoffee() {
        CoffeeTestFixture.deleteTable();
    }
}
