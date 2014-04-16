package com.airhacks.enhydrator.flexpipe;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class PlumberTest {

    @Test
    public void writeAndReadConfiguration() {
        Plumber plumber = new Plumber();
        JDBCPipeline origin = JDBCPipelineTest.getJDBCPipeline();
        plumber.intoConfiguration(origin);
        Pipeline deserialized = plumber.fromConfiguration(origin.getName());
        assertNotSame(deserialized, origin);
        assertThat(deserialized, is(origin));
    }

}
