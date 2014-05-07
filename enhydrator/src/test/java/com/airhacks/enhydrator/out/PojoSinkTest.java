package com.airhacks.enhydrator.out;

import com.airhacks.enhydrator.in.Row;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class PojoSinkTest {

    PojoSink cut;

    CachingConsumer cachingConsumer;

    @Before
    public void init() {
        this.cachingConsumer = new CachingConsumer();
        this.cut = new PojoSink(Developer.class, this.cachingConsumer);
    }

    @Test
    public void stringMapping() {
        final String origin = "duke";
        Row row = new Row();
        row.addColumn("name", origin);
        this.cut.processRow(row);
        Developer dev = getDeveloper();
        String name = dev.getName();
        assertThat(origin, is(name));
    }

    private Developer getDeveloper() {
        return (Developer) this.cachingConsumer.getObject();
    }

}
