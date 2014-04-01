package com.airhacks.enhydrator.in;

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
        Source cut = new Source.Configuration().
                driver("org.apache.derby.jdbc.EmbeddedDriver").
                url("jdbc:derby:./coffees;create=true").
                newSource();
        assertNotNull(cut);
    }

    @Test(expected = IllegalStateException.class)
    public void constructInvalidSource() {
        Source cut = new Source.Configuration().
                driver("org.airhacks.driver").
                url("outer-space").
                newSource();
        assertNotNull(cut);

    }

}
