package com.airhacks.enhydrator.scenarios;

/*
 * #%L
 * enhydrator
 * %%
 * Copyright (C) 2014 - 2015 Adam Bien
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
import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.transform.Datatype;
import com.airhacks.enhydrator.transform.DatatypeNameMapper;
import com.airhacks.enhydrator.transform.Memory;
import com.airhacks.enhydrator.transform.SkipFirstRow;
import java.util.List;
import org.junit.Assert;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class ReadJDKVersionsTest {

    @Test
    public void readFromCSV() {
        Source input = new CSVFileSource("./src/test/files/jdk-dates.csv", ";", "UTF-8", true);
        VirtualSinkSource output = new VirtualSinkSource();

        Pump pump = new Pump.Engine().
                from(input).
                startWith(new SkipFirstRow()).
                startWith(new DatatypeNameMapper().
                        addMapping("Year", Datatype.INTEGER)).
                filter("$ROW.getColumnValue('Year') > 2000").
                to(new LogSink()).
                to(output).build();
        Memory memory = pump.start();
        Assert.assertFalse(memory.areErrorsOccured());

        List<Row> rows = output.getRows();
        assertFalse(rows.isEmpty());
        Row first = rows.get(1);
        Object year = first.getColumnValue("Year");
        System.out.println("The year is: " + year);
    }

}
