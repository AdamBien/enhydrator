package com.airhacks.samples.json;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.CSVFileSource;
import com.airhacks.enhydrator.in.Row;
import com.airhacks.enhydrator.out.CSVFileSink;
import com.airhacks.enhydrator.out.LogSink;
import com.airhacks.enhydrator.out.PojoSink;
import com.airhacks.enhydrator.out.RowSink;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

/**
 * @author ZeTo
 */
public class CsvCharsetTest extends CsvTest {

    String sourceDelimiter = "*";
    String sourceCharsetName = "UTF-8";
    boolean sourceContainsHeaders = true;

    String sinkName = "*";
    String sinkDelimiter = "#";
    boolean sinkUseNamesAsHeaders = true;
    boolean sinkAppend = true;

    @Test
    public void fromJSONToCSVWithCharset() throws FileNotFoundException {

        CSVFileSource source = new CSVFileSource(INPUT + "utf8-characters.csv", sourceDelimiter, sourceCharsetName, sourceContainsHeaders);

        String fileName = getFileName();
        final CSVFileSink csvFileSink = new CSVFileSink(sinkName, fileName, sinkDelimiter, sinkUseNamesAsHeaders, sinkAppend, "UTF-8");

        // do the test
        Pump pumpOut = new Pump.Engine().
                from(source).
                to(new LogSink()).
                to(csvFileSink).
                build();
        pumpOut.start();
    }
}
