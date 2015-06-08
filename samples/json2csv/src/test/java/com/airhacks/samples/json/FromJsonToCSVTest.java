package com.airhacks.samples.json;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.ScriptableSource;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.in.VirtualSinkSource;
import com.airhacks.enhydrator.out.CSVFileSink;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.transform.Datatype;
import com.airhacks.enhydrator.transform.DatatypeNameMapper;
import com.airhacks.enhydrator.transform.Memory;
import java.io.FileNotFoundException;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class FromJsonToCSVTest extends CsvTest {

    @Test
    public void fromJSONToCSV() throws FileNotFoundException {
        Source source = new ScriptableSource(INPUT + "languages.json",
                INPUT + "converter.js", "UTF-8");
        final CSVFileSink csvFileSink = new CSVFileSink("*", getFileName(), ";", true, false);

        Pump pump = new Pump.Engine().
                from(source).
                to(new LogSink()).
                to(csvFileSink).
                build();
        pump.start();
    }

    @Test
    public void filterAndCastFromCSVFileToLog() {
        Source source = new CSVFileSource(INPUT + "/languages.csv", ";", "utf-8", true);
        VirtualSinkSource sink = new VirtualSinkSource();
        Pump pump = new Pump.Engine().
                from(source).
                filter("$ROW.getColumnValue('language') === 'java'").
                startWith(new DatatypeNameMapper().addMapping("rank", Datatype.INTEGER)).
                to(sink).
                to(new LogSink()).
                build();
        Memory memory = pump.start();
        assertFalse(memory.areErrorsOccured());
        assertThat(memory.getProcessedRowCount(), is(5l));

        //expecting only "java" language
        assertThat(sink.getNumberOfRows(), is(1));
        String languageValue = (String) sink.getRow(0).getColumnValue("language");
        assertThat(languageValue, is("java"));

        //expecting "java" having rank 1 as Integer
        Object rankValue = sink.getRow(0).getColumnValue("rank");
        assertTrue(rankValue instanceof Integer);
        assertThat((Integer) rankValue, is(1));
    }

}
