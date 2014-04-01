package com.airhacks.enhydrator.in;

import java.sql.ResultSet;
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
    public void queryExecution() {
        ResultSet result = getSource().query("select * from Coffee");
        assertNotNull(result);
    }

}
