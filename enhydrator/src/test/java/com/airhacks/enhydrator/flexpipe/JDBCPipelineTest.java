package com.airhacks.enhydrator.flexpipe;

import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.in.JDBCSourceTest;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class JDBCPipelineTest {

    @Test
    public void jaxbSerialization() {
        JDBCSource source = JDBCSourceTest.getSource();
        JDBCPipeline origin = new JDBCPipeline("tst", source);
        Plumber plumber = new Plumber(".", "config");
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertEquals(deserialized, origin);
    }

}
