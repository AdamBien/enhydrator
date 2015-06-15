package com.airhacks.enhydrator.scenarios;

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
                startWith(new DatatypeNameMapper().addMapping("Year", Datatype.INTEGER)).
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
