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
        final String expected = "duke";
        Row row = new Row();
        row.addColumn("name", expected);
        this.cut.processRow(row);
        Developer dev = getDeveloper();
        String actual = dev.getName();
        assertThat(actual, is(expected));
    }

    @Test
    public void intMapping() {
        final int expected = 42;
        Row row = new Row();
        row.addColumn("age", expected);
        this.cut.processRow(row);
        Developer dev = getDeveloper();
        int actual = dev.getAge();
        assertThat(actual, is(expected));
    }

    private Developer getDeveloper() {
        return (Developer) this.cachingConsumer.getObject();
    }

}
