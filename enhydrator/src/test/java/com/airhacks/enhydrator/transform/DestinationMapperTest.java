package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Column;
import com.airhacks.enhydrator.in.Row;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class DestinationMapperTest {

    DestinationMapper cut;

    @Before
    public void init() {
        this.cut = new DestinationMapper();
    }

    @Test
    public void mapping() {
        Row input = new Row();
        final int INDEX = 0;
        input.addColumn(INDEX, "name", "duke");
        final String expectedSink = "customSink";
        final String expectedObject = "targetObject";
        this.cut.addMapping(INDEX, new Mapping(expectedSink, expectedObject));
        Row output = this.cut.execute(input);
        Column column = output.getColumnByIndex(INDEX);
        assertThat(column.getTargetObject(), is(expectedObject));
        assertThat(column.getTargetSink(), is(expectedSink));
    }

}
