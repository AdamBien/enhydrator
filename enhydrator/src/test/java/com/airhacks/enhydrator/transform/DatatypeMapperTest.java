package com.airhacks.enhydrator.transform;

import com.airhacks.enhydrator.in.Row;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class DatatypeMapperTest {

    DatatypeMapper cut;

    @Before
    public void init() {
        this.cut = new DatatypeMapper();
    }

    @Test
    public void doubleMapping() {
        Row input = getRow();
        this.cut.addMapping(0, Datatype.DOUBLE);
        Row output = this.cut.execute(input);
        assertTrue(output.getColumnByIndex(0).getValue() instanceof Double);
    }

    @Test
    public void integerMapping() {
        Row input = getRow();
        this.cut.addMapping(0, Datatype.INTEGER);
        Row output = this.cut.execute(input);
        assertTrue(output.getColumnByIndex(0).getValue() instanceof Integer);
    }

    Row getRow() {
        Row row = new Row();
        return row.addColumn(0, "name", "42");
    }

}
