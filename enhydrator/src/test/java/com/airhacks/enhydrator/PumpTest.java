package com.airhacks.enhydrator;

import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.transform.Memory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class PumpTest {

    @Test
    public void type() {
        List<Row> inputRows = new ArrayList<>();
        inputRows.add(getStringRow());
        inputRows.add(getIntRow());
        VirtualSinkSource in = new VirtualSinkSource("in", inputRows);
        VirtualSinkSource out = new VirtualSinkSource();
        Pump cut = new Pump.Engine().
                from(in).
                startWith(t -> {
                    t.getColumnByName("a").convertToInteger();
                    return t;
                }).
                to(out).
                build();
        Memory memory = cut.start();
        assertTrue(memory.areErrorsOccured());
        Set<Row> erroneousRows = memory.getErroneousRows();
        assertNotNull(erroneousRows);
        assertFalse(erroneousRows.isEmpty());
        assertThat(memory.getProcessedRowCount(), is(1l));

    }

    Row getStringRow() {
        Row row = new Row();
        row.addColumn(0, "a", "java");
        row.addColumn(1, "b", "tengah");
        return row;
    }

    Row getIntRow() {
        Row row = new Row();
        row.addColumn(0, "a", "1");
        row.addColumn(1, "b", "2");
        return row;
    }

}
