package com.airhacks.enhydrator.scenarios;

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
import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.Entry;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.LogSink;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class SplitEntriesAcrossSinksTest {

    @Test
    public void splitWithoutTransformation() {
        final Entry tesla = new Entry(0, "name", "tesla").changeDestination("electric");
        final Entry touran = new Entry(0, "name", "touran").changeDestination("diesel");
        Source source = new VirtualSinkSource.Rows().
                addColumn(tesla).
                addRow().
                addColumn(touran).
                addRow().
                build();

        VirtualSinkSource electric = new VirtualSinkSource.Rows().sinkName("electric").build();
        VirtualSinkSource diesel = new VirtualSinkSource.Rows().sinkName("diesel").build();

        Pump pump = new Pump.Engine().
                flowListener(l -> System.out.println(l)).
                from(source).
                to(diesel).
                to(electric).
                to(new LogSink()).
                build();
        pump.start();
        System.out.println("Electric: " + electric.toString());
        System.out.println("Diesel: " + diesel.toString());
        List<Entry> first = electric.getRow(0);
        assertFalse(first.isEmpty());
        assertThat(first.get(0), is(tesla));

        assertThat(electric.getNumberOfRows(), is(1));
        assertThat(diesel.getNumberOfRows(), is(1));

    }

}
