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
        JDBCPipeline origin = getJDBCPipeline();
        Plumber plumber = new Plumber(".", "config");
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertEquals(deserialized, origin);
    }

    public static JDBCPipeline getJDBCPipeline() {
        JDBCSource source = JDBCSourceTest.getSource();
        JDBCSink sink = JDBCSinkTest.getSink();
        EntryTransformation e1 = new EntryTransformation("name", "convert", true);
        EntryTransformation e2 = new EntryTransformation(42, "compress", true);
        JDBCPipeline origin = new JDBCPipeline("tst", "./src/test/scripts", "select * from Coffee where name like ? and strength = ?", source, sink);
        origin.addQueryParam("arabica");
        origin.addQueryParam(2);
        origin.addEntryTransformation(e1);
        origin.addEntryTransformation(e2);
        origin.addPreRowTransforation("reverse");
        origin.addPreRowTransforation("validate");
        origin.addPostRowTransformation("compress");
        origin.addPostRowTransformation("encrypt");
        origin.addExpression("print(current); java.util.Collections.emptyList();");
        return origin;
    }

}
