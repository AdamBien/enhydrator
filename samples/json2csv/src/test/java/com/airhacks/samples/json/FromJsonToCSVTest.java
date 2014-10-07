package com.airhacks.samples.json;

import com.airhacks.enhydrator.Pump;
import com.airhacks.enhydrator.in.ScriptableSource;
import com.airhacks.enhydrator.in.Source;
import com.airhacks.enhydrator.out.CSVFileSink;
import com.airhacks.enhydrator.out.LogSink;
import java.io.FileNotFoundException;
import org.junit.Test;

/**
 *
 * @author airhacks.com
 */
public class FromJsonToCSVTest {

    private final static String INPUT = "./src/test/resources/";

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

    public String getFileName() {
        return "./target/" + System.currentTimeMillis() + "output.csv";
    }

}
