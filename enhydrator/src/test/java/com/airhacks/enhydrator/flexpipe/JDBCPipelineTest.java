package com.airhacks.enhydrator.flexpipe;

import com.airhacks.enhydrator.in.JDBCSource;
import com.airhacks.enhydrator.in.JDBCSourceTest;
import com.airhacks.enhydrator.out.JDBCSink;
import com.airhacks.enhydrator.out.JDBCSinkTest;
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
        JDBCSink sink = JDBCSinkTest.getSink();
        JDBCPipeline origin = new JDBCPipeline("tst", source, sink);
        Plumber plumber = new Plumber(".", "config");
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertEquals(deserialized, origin);
    }

}
