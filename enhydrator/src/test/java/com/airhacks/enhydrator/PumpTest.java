package com.airhacks.enhydrator;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 Adam Bien
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.transform.Memory;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public void continueOnError() {
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
                continueOnError().
                build();
        Memory memory = cut.start();
        assertTrue(memory.areErrorsOccured());
        Set<Row> erroneousRows = memory.getErroneousRows();
        assertNotNull(erroneousRows);
        assertFalse(erroneousRows.isEmpty());
        assertThat(memory.getProcessedRowCount(), is(1l));
    }

    @Test(expected = NumberFormatException.class)
    public void stopOnError() {
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
        cut.start();
    }

    @Test
    public void scriptEngineBindings() {
        List<Row> inputRows = new ArrayList<>();
        inputRows.add(getStringRow());
        Map<String, Object> bindings = new HashMap<>();
        bindings.put("date", new Date());

        VirtualSinkSource in = new VirtualSinkSource("in", inputRows);
        VirtualSinkSource out = new VirtualSinkSource();
        Pump cut = new Pump.Engine().
                homeScriptFolder("src/test/scripts", bindings).
                from(in).
                startWith("date_should_exist").
                to(out).
                build();
        Memory memory = cut.start();
        assertNotNull(memory.get("date"));

    }

    Row getStringRow() {
        Row row = new Row();
        row.useMemory(new Memory());
        row.addColumn(0, "a", "java");
        row.addColumn(1, "b", "tengah");
        return row;
    }

    Row getIntRow() {
        Row row = new Row();
        row.useMemory(new Memory());
        row.addColumn(0, "a", "1");
        row.addColumn(1, "b", "2");
        return row;
    }

}
